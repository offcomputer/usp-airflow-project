from abc import ABC, abstractmethod

class DataSinkPattern(ABC):
    """
    Abstract base class for data sinks. Further methods must be
    implemented here, before to apply in the subclasses.
    """
    
    @abstractmethod
    def load_to_sink(self) -> None:
        """
        Load data to the sink.
        """
        ...