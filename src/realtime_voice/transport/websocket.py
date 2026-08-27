"""WebSocket 握手与协议错误处理。"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from fastapi import WebSocket, WebSocketDisconnect

from realtime_voice.protocol.client_messages import CreateSession
from realtime_voice.protocol.decoder import decode_client_message
from realtime_voice.protocol.errors import ProtocolViolation
from realtime_voice.protocol.server_messages import ErrorMessage
from realtime_voice.session.runtime import SlowClient

if TYPE_CHECKING:
    from realtime_voice.main import AppServices


async def serve_realtime(websocket: WebSocket, services: AppServices) -> None:
    """接受 WebSocket 连接，并要求首帧必须是 CREATE_SESSION 消息。"""
    await websocket.accept()
    try:
        try:
            raw = await asyncio.wait_for(
                websocket.receive_text(), timeout=services.settings.handshake_timeout_seconds
            )
        except KeyError as error:
            raise ProtocolViolation(
                "INVALID_MESSAGE", "message must be a JSON text frame"
            ) from error
        create = require_create_session(decode_client_message(raw))
        runtime = await services.registry.create(create, websocket)
        from realtime_voice.transport.messages import session_created

        await runtime.outbound.put(session_created(create))
        try:
            await runtime.run()
        except* ProtocolViolation as errors:
            # runtime.run 抛出的协议违规，转为关闭流程
            error = errors.exceptions[0]
            await _close_with_protocol_error(websocket, error)
        except* SlowClient:
            # 出站积压已满，按慢客户端协议违规关闭
            await _close_with_protocol_error(
                websocket,
                ProtocolViolation("SLOW_CLIENT", "outbound client backlog is full"),
            )
    except ProtocolViolation as error:
        await _close_with_protocol_error(websocket, error)
    except TimeoutError:
        await _close_with_protocol_error(
            websocket,
            ProtocolViolation("HANDSHAKE_TIMEOUT", "CREATE_SESSION was not received in time"),
        )
    except WebSocketDisconnect:
        return


def require_create_session(message: object) -> CreateSession:
    """校验消息为协议规定的开场消息 CREATE_SESSION，否则抛出协议违规。"""
    if not isinstance(message, CreateSession):
        raise ProtocolViolation("CREATE_SESSION_REQUIRED", "first message must be CREATE_SESSION")
    return message


async def _close_with_protocol_error(websocket: WebSocket, error: ProtocolViolation) -> None:
    """尽力发送 ERROR 并按策略关闭连接；连接可能已断开，故容错。"""
    await send_protocol_error(websocket, error)
    try:
        await websocket.close(code=1008)
    except (WebSocketDisconnect, RuntimeError):
        return


async def send_protocol_error(websocket: WebSocket, error: ProtocolViolation) -> None:
    """在关闭异常连接前发送一条稳定的传输层错误消息。"""
    message = ErrorMessage(
        type="ERROR",
        user_id="unknown",
        session_id="unknown",
        turn_id=0,
        interrupt=False,
        stage="TRANSPORT",
        code=error.code,
        message=error.message,
        recoverable=False,
    )
    payload = message.model_dump_json()
    try:
        await websocket.send_text(payload)
    except (WebSocketDisconnect, RuntimeError):
        return
