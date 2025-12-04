from patterns.etl.load import LoaderPattern
import services.message_broker as message_broker
import services.data_sink as data_sink

class Loader(LoaderPattern):
    """
    Loader class that implements the LoaderPattern.
    This class is responsible for simulating the loading of data
    from a message broker to a data sink.
    """

    def __init__(self):
        """
        Initialize the Transformer class.
        """
        self.broker = message_broker.MessageBroker()
        self.sink = data_sink.DataSink()

    def consume_from_broker(self):
        """
        Simulate the consumption of data from a message broker.
        """
        self.broker.consume()
    
    def load_data(self):
        """
        Simulate the loading of data to a data sink.
        """
        self.sink.load_to_sink()