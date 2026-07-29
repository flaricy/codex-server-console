from __future__ import annotations


class ConsoleError(Exception):
    def __init__(self, code: str, message: str, *, status: int = 400) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.status = status

    def as_dict(self) -> dict[str, object]:
        return {"error": {"code": self.code, "message": self.message}}


class ConflictError(ConsoleError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(code, message, status=409)


class NotFoundError(ConsoleError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(code, message, status=404)

