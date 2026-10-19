from __future__ import annotations


ERROR_STATUS: dict[str, int] = {
    "NOT_FOUND": 404,
    "TASK_NOT_FOUND": 404,
    "AGENT_DISABLED": 409,
    "AGENT_BUSY": 409,
    "TASK_ALREADY_TERMINAL": 409,
    "AGENT_NOT_DISABLED": 409,
    "INVALID_INPUT": 422,
    "INVALID_TERMINAL_STATUS": 422,
    "INVALID_STATE": 422,
    "TASK_REQUIRED": 422,
    "TASK_NOT_ALLOWED": 422,
    "BUFFER_DEBUG_NOT_CONFIGURED": 503,
    "BUFFER_DEBUG_UNAUTHORIZED": 401,
}


class DomainError(Exception):
    """An error with a stable machine-readable code from the API contract."""

    def __init__(self, status_code: int, code: str, message: str = "") -> None:
        self.status_code = status_code
        self.code = code
        self.message = message or code
        super().__init__(self.message)

    @classmethod
    def of(cls, code: str) -> DomainError:
        return cls(ERROR_STATUS[code], code)
