from apps.template.etl.extract import Extractor
from apps.template.etl.transform import Transformer
from apps.template.etl.load import Loader
import library.helpers as helpers


def extract(task_type: str = "extractors"):
    """
    Extract data from a data source and publish it to a message broker.
    """
    for n, n_task_batches in helpers.batches(task_type):
        start = helpers.start_delta_seconds()
        extractor = Extractor()
        extractor.get_data()
        extractor.publish_to_broker()
        stop = helpers.stop_delta_seconds(start)
        helpers.get_log(n, n_task_batches, stop)

def transform(task_type: str = "transformers"):
    """
    Consume data from a message broker, transform it, and publish it back
    to the message broker.
    """
    for n, n_task_batches in helpers.batches(task_type):
        start = helpers.start_delta_seconds()
        transformer = Transformer()
        transformer.consume_from_broker()
        transformer.transform_data()
        transformer.publish_to_broker()
        stop = helpers.stop_delta_seconds(start)
        helpers.get_log(n, n_task_batches, stop)

def load(task_type: str = "loaders"):
    """
    Consume data from a message broker and load it to a data sink.
    """
    for n, n_task_batches in helpers.batches(task_type):
        start = helpers.start_delta_seconds()
        loader = Loader()
        loader.consume_from_broker()
        loader.load_data()
        stop = helpers.stop_delta_seconds(start)
        helpers.get_log(n, n_task_batches, stop)
