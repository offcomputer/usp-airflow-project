from apps.template.etl.extract import Extractor
from apps.template.etl.transform import Transformer
from apps.template.etl.load import Loader
import library.helpers as helpers
from airflow.utils.log.logging_mixin import LoggingMixin
logger = LoggingMixin().log

def batches():
    for n in range(helpers.get_n_batches()):
        n += 1
        yield n
    
def extract():
    """
    Extract data from a data source and publish it to a message broker.
    """
    for n in batches():
        extractor = Extractor()
        logger.info(f'start: {n}')
        extractor.get_data()
        extractor.publish_to_broker()
        logger.info(f'stop: {n}')

def transform():
    """
    Consume data from a message broker, transform it, and publish it back
    to the message broker.
    """
    for n in batches():
        transformer = Transformer()
        logger.info(f'start: {n}')
        transformer.consume_from_broker()
        transformer.transform_data()
        transformer.publish_to_broker()
        logger.info(f'stop: {n}')

def load():
    """
    Consume data from a message broker and load it to a data sink.
    """
    for n in batches():
        loader = Loader()
        logger.info(f'start: {n}')
        loader.consume_from_broker()
        loader.load_data()
        logger.info(f'stop: {n}')
