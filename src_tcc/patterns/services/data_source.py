from abc import ABC, abstractmethod
from typing import Any

class DataSourcePattern(ABC):
    """
    Abstract base class for data sources. Further methods must be
    implemented here, before to apply in the subclasses.
    """
    
    @abstractmethod
    def extract_from_source(self) -> Any:
        """
        Extract data from the data source.
        """
        ...