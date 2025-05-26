from patterns.etl.transform import TransformerPattern
import services.message_broker as message_broker

class Transformer(TransformerPattern):
    """
    Transformer class that implements the TransformerPattern.
    This class is responsible for simulating the transformation of data
    from a data source and publishing it to a message broker.
    """

    def __init__(self):
        """
        Initialize the Transformer class.
        """
        self.broker = message_broker.MessageBroker()

    def consume_from_broker(self):
        """
        Simulate the consumption of data from a message broker.
        """
        self.broker.consume()
    
    def publish_to_broker(self):
        """
        Simulate the publishing of data to a message broker.
        """
        self.broker.publish()