from abc import ABC, abstractmethod
from typing import (
    Any, 
    Iterable, 
    Mapping, 
    Optional, 
    Protocol, 
    runtime_checkable
    )


@runtime_checkable
class SupportsId(Protocol):
    """Simple protocol for objects that expose an `id` attribute."""

    @property
    def id(self) -> Any:
        ...
    

class ObjectRepoCRUDInterface(ABC):
    """Abstract interface for basic CRUD operations on a storage 
    repository."""

    @abstractmethod
    def create(self, item: Mapping[str, Any]) -> Any:
        """Create a new item in the repository and return its identifier 
        or representation."""
        raise NotImplementedError

    @abstractmethod
    def read(self, item_id: Any) -> Optional[Mapping[str, Any]]:
        """Retrieve a single item by identifier. Returns None if not 
        found."""
        raise NotImplementedError

    @abstractmethod
    def update(self, 
               item_id: Any, 
               updates: Mapping[str, Any]
               ) -> Mapping[str, Any]:
        """Update an existing item and return the updated 
        representation."""
        raise NotImplementedError

    @abstractmethod
    def delete(self, 
               item_id: Any
               ) -> None:
        """Delete an item from the repository."""
        raise NotImplementedError


class ObjectRepoReportsInterface(ABC):
    """Abstract interface for read-only reporting-style operations on a 
    storage repository."""

    @abstractmethod
    def count(self, *, 
              filters: Optional[Mapping[str, Any]] = None
              ) -> int:
        """Return a count of items that match the given filters."""
        raise NotImplementedError

    @abstractmethod
    def exists(self, item_id: Any) -> bool:
        """Return True if an item with the given identifier exists."""
        raise NotImplementedError

    @abstractmethod
    def list_ids(self, *, 
                 limit: Optional[int] = None, offset: int = 0
                 ) -> Iterable[Any]:
        """Return an iterable of item identifiers, optionally 
        paginated."""
        raise NotImplementedError

    @abstractmethod
    def summarize(self) -> Mapping[str, Any]:
        """Return a summary of repository contents, suitable for 
        reporting/monitoring."""
        raise NotImplementedError


class ObjectRepoManagerInterface(ABC):
    """Abstract interface for managing logical repositories/buckets in 
    the underlying storage system."""

    @abstractmethod
    def create_repo(self, 
                    repo_id: str, *, 
                    metadata: Optional[Mapping[str, Any]] = None) -> None:
        """Create a new logical repository (e.g., bucket or prefix) in 
        the backing storage system."""
        raise NotImplementedError

    @abstractmethod
    def delete_repo(self, repo_id: str, *, force: bool = False) -> None:
        """Delete an existing logical repository from the backing storage
        system."""
        raise NotImplementedError
