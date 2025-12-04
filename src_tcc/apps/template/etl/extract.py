from patterns.etl.extract import ExtractorPattern
import services.message_broker as message_broker
import services.data_source as data_source

class Extractor(ExtractorPattern):
    """
    Extractor class that implements the ExtractorPattern.
    This class is responsible for simulating the extraction of data
    from a data source and publishing it to a message broker.
    """
    
    def __init__(self):
        """
        Initialize the Transformer class.
        """
        self.broker = message_broker.MessageBroker()
        self.source = data_source.DataSource()

    def get_data(self):
        """
        Simulate the extraction of data from a data source.
        """
        self.source.extract_from_source()
    
    def publish_to_broker(self):
        """
        Simulate the publishing of data to a message broker.
        """
        self.broker.publish()