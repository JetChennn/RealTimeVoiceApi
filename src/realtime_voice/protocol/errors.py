"""Stable protocol-level errors exposed by the transport boundary."""


class ProtocolViolation(ValueError):
    """A client message violates the V1 protocol."""

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")
