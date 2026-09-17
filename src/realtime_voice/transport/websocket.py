"""WebSocket 握手与协议错误处理。"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from fastapi import WebSocket, WebSocketDisconnect

from realtime_voice.observability.logging import log_event
from realtime_voice.protocol.client_messages import CreateSession
from realtime_voice.protocol.decoder import decode_client_message
from realtime_voice.protocol.errors import ProtocolViolation
from realtime_voice.protocol.server_messages import ErrorMessage
from realtime_voice.session.registry import DuplicateSession, SessionCapacityExceeded
from realtime_voice.session.runtime import SlowClient

if TYPE_CHECKING:
    from realtime_voice.main import AppServices


async def serve_realtime(websocket: WebSocket, services: AppServices) -> None:
    """接受 WebSocket 连接，并要求首帧必须是 CREATE_SESSION 消息。"""
    await websocket.accept()
    create: CreateSession | None = None
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
        try:
            runtime = await services.registry.create(create, websocket)
        except (ProtocolViolation, DuplicateSession, SessionCapacityExceeded, WebSocketDisconnect):
            raise
        except Exception as error:  # noqa: BLE001 - initialization failures become protocol errors
            log_event(
                "session_create_failed",
                logger=logging.getLogger(__name__),
                level=logging.ERROR,
                exc_info=error,
                device_id=create.device_id,
                user_id=create.device_id,
                session_id=create.session_id,
                stage="TRANSPORT",
                error_code="SESSION_CREATE_FAILED",
                error_type=type(error).__name__,
            )
            await _close_with_protocol_error(
                websocket,
                ProtocolViolation(
                    "SESSION_CREATE_FAILED",
                    "server could not initialize the session; please retry later",
                ),
                close_code=1011,
                create=create,
            )
            return
        from realtime_voice.transport.messages import session_created

        await runtime.outbound.put(session_created(create))
        try:
            await runtime.run()
        except* ProtocolViolation as errors:
            # runtime.run 抛出的协议违规，转为关闭流程
            error = errors.exceptions[0]
            _log_protocol_error(error, create)
            await _close_with_protocol_error(websocket, error, create=create)
        except* SlowClient:
            # 出站积压已满，按慢客户端协议违规关闭
            error = ProtocolViolation("SLOW_CLIENT", "outbound client backlog is full")
            _log_protocol_error(error, create)
            await _close_with_protocol_error(
                websocket,
                error,
                create=create,
            )
        except* Exception as errors:  # noqa: BLE001 - runtime failures become protocol errors
            error = errors.exceptions[0]
            log_event(
                "session_runtime_failed",
                logger=logging.getLogger(__name__),
                level=logging.ERROR,
                exc_info=error,
                device_id=create.device_id,
                user_id=create.device_id,
                session_id=create.session_id,
                stage="TRANSPORT",
                error_code="SESSION_RUNTIME_FAILED",
                error_type=type(error).__name__,
            )
            await _close_with_protocol_error(
                websocket,
                ProtocolViolation("SESSION_RUNTIME_FAILED", "session stopped unexpectedly"),
                close_code=1011,
                create=create,
            )
    except ProtocolViolation as error:
        _log_protocol_error(error, create)
        await _close_with_protocol_error(websocket, error, create=create)
    except (DuplicateSession, SessionCapacityExceeded) as error:
        violation = ProtocolViolation(
            error.code,
            "session_id is already active"
            if isinstance(error, DuplicateSession)
            else "active session capacity is exhausted; retry after a session closes",
        )
        _log_protocol_error(violation, create)
        await _close_with_protocol_error(
            websocket,
            violation,
            create=create,
        )
    except TimeoutError:
        error = ProtocolViolation("HANDSHAKE_TIMEOUT", "CREATE_SESSION was not received in time")
        _log_protocol_error(error, create)
        await _close_with_protocol_error(
            websocket,
            error,
            create=create,
        )
    except WebSocketDisconnect:
        if create is not None:
            log_event(
                "client_disconnected",
                logger=logging.getLogger(__name__),
                device_id=create.device_id,
                user_id=create.device_id,
                session_id=create.session_id,
                stage="TRANSPORT",
            )
        return


def require_create_session(message: object) -> CreateSession:
    """校验消息为协议规定的开场消息 CREATE_SESSION，否则抛出协议违规。"""
    if not isinstance(message, CreateSession):
        raise ProtocolViolation("CREATE_SESSION_REQUIRED", "first message must be CREATE_SESSION")
    return message


async def _close_with_protocol_error(
    websocket: WebSocket,
    error: ProtocolViolation,
    *,
    close_code: int = 1008,
    create: CreateSession | None = None,
) -> None:
    """尽力发送 ERROR 并按策略关闭连接；连接可能已断开，故容错。"""
    await send_protocol_error(websocket, error, create=create)
    try:
        await websocket.close(code=close_code, reason=error.code)
    except (WebSocketDisconnect, RuntimeError, OSError):
        return


async def send_protocol_error(
    websocket: WebSocket,
    error: ProtocolViolation,
    *,
    create: CreateSession | None = None,
) -> None:
    """在关闭异常连接前发送一条稳定的传输层错误消息。"""
    message = ErrorMessage(
        type="ERROR",
        user_id=create.device_id if create is not None else "unknown",
        session_id=create.session_id if create is not None else "unknown",
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
    except (WebSocketDisconnect, RuntimeError, OSError):
        return


def _log_protocol_error(error: ProtocolViolation, create: CreateSession | None) -> None:
    try:
        log_event(
            "protocol_error",
            logger=logging.getLogger(__name__),
            level=logging.WARNING,
            device_id=create.device_id if create is not None else "unknown",
            user_id=create.device_id if create is not None else "unknown",
            session_id=create.session_id if create is not None else "unknown",
            stage="TRANSPORT",
            error_code=error.code,
            error_type=type(error).__name__,
        )
    except Exception:  # noqa: BLE001 - transport behavior must not depend on logging
        return
