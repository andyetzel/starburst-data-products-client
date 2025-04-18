class ApiException(Exception):
    def __init__(self, message, reason: str, status: int, body: str):
        super().__init__(message)
        self.reason = reason
        self.status = status
        self.body = body
