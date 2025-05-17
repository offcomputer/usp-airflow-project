from abc import ABC, abstractmethod
from typing import Any

class MessageBrokerPattern(ABC):
    """
    Abstract base class for message brokers. Further methods must be 
    implemented here, before to apply in the subclasses.
    """
    
    @abstractmethod
    def publish(self) -> None:
        """
        Publish a message to the broker.
        """
        ...

    @abstractmethod
    def consume(self) -> Any:
        """
        Consume a message from the broker.
        """
        ...

    @abstractmethod
    def acknowledge(self) -> None:
        """
        Acknowledge a message in the broker.
        """
        ...

    @abstractmethod
    def delete(self, *args, **kwargs) -> None:
        """
        Delete a message from the broker.
        """
        ...