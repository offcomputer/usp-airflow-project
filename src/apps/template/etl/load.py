from patterns.etl.load import LoaderPattern

class Loader(LoaderPattern):
    """
    Loader class that implements the LoaderPattern.
    This class is responsible for simulating the loading of data
    from a message broker to a data sink.
    """

    def consume_from_broker(self):
        """
        Simulate the consumption of data from a message broker.
        """
        ...
    
    def load_data(self):
        """
        Simulate the loading of data to a data sink.
        """
        ...