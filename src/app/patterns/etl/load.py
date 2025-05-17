from abc import ABC, abstractmethod
from typing import Any

class LoaderPattern(ABC):
    """
    Abstract base class for loaders. Further methods must be
    implemented here, before to apply in the subclasses.
    """

    def __init__(self, client: Any) -> None:
        """
        Initialize the loader with a client.
        """
        self.client = client
    
    @abstractmethod
    def consume_from_broker(self) -> Any:
        """
        Consume a message from the broker.
        """
        ...

    @abstractmethod
    def load_data(self) -> None:
        """
        Load a data batch to the data sink.
        """
        ...