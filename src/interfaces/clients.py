from abc import ABC, abstractmethod
from typing import Any

class ServiceClientInterface(ABC):
    """Abstract interface for managing clients (e.g. S3, MinIO)."""

    @abstractmethod
    def create_client(self, **kwargs: Any) -> Any:
        """Create and return an underlying storage client instance."""
        raise NotImplementedError

    @abstractmethod
    def delete_client(self, client: Any) -> None:
        """Dispose of / close the given storage client instance."""
        raise NotImplementedError