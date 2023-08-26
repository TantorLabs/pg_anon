from enum import Enum


class StrEnum(str, Enum):
    """A shorthand for a string enum required for Python versions lower than 3.11."""
    def __str__(self):
        return str(self.value)
