from abc import ABC, abstractmethod
from typing import Any

class TransformerPattern(ABC):
    """
    Abstract base class for transformers. Further methods must be
    implemented here, before to apply in the subclasses.
    """
    
    @abstractmethod
    def consume_from_broker(self) -> Any:
        """
        Consume a message from the broker.
        """
        ...

    @abstractmethod
    def publish_to_broker(self) -> None:
        """
        Publish a data batch to the message broker.
        """
        ...