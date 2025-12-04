from patterns.services.data_sink import DataSinkPattern
import library.helpers as helpers

class DataSink(DataSinkPattern):
    """
    DataSink class that extends the DataSinkPattern.
    This class is responsible for writing data to a sink.
    """

    def load_to_sink(self):
        """
        Load data to the sink.
        """
        helpers.simulate_service_time('external_services')