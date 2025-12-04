from patterns.services.data_source import DataSourcePattern
import library.helpers as helpers

class DataSource(DataSourcePattern):
    """
    DataSource class that extends the DataSourcePattern.
    This class is responsible for extracting data from the source.
    """

    def extract_from_source(self):
        """
        Extract data from the source.
        """
        helpers.simulate_service_time('external_services')