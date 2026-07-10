class DglabError(Exception):
    """SDK error with the same named-error shape as the TypeScript package."""

    def __init__(self, name: str, message: str) -> None:
        super().__init__(message)
        self.name = f"DGLAB-{name}"

    def __str__(self) -> str:
        message = super().__str__()
        return f"{self.name}: {message}" if message else self.name


def create_named_error(name: str, message: str) -> DglabError:
    return DglabError(name, message)
