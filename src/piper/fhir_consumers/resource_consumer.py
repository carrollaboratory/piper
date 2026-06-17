from abc import ABC, abstractmethod
from types import TracebackType
from typing import Any, Optional, Type


class ResourceConsumer(ABC):
    """Abstract base for resource consumers (Callable)."""

    @abstractmethod
    def __call__(
        self, template_name: str, resource: dict[str, Any], payload: dict[str, Any]
    ):
        """This will be called for each resource generated"""
        pass

    def reset(
        self, title: str, report_locals: bool = True, console=None
    ) -> None | dict[str, int]:
        """Optional reset for summarys or to add organization to the internal resource structure."""
        pass
