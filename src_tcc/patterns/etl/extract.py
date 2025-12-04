from abc import ABC, abstractmethod
from typing import Any

class ExtractorPattern(ABC):
    """
    Abstract base class for extractors. Further methods must be
    implemented here, before to apply in the subclasses.
    """

    def __init__(self, client: Any) -> None:
        """
        Initialize the extractor with a client.
        """
        self.client = client
    
    @abstractmethod
    def get_data(self) -> Any:
        """
        Get a data batch from the data source.
        """
        ...

    @abstractmethod
    def publish_to_broker(self) -> None:
        """
        Publish a data batch to the message broker.
        """
        ...