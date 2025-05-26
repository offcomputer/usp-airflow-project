from patterns.services.message_broker import MessageBrokerPattern
import library.helpers as helpers

class MessageBroker(MessageBrokerPattern):
    """
    Message broker class that implements the MessageBrokerPattern.
    This class simulates the message broker functionality.
    """

    def publish(self) -> None:
        """
        Publish a message to the broker.
        """
        helpers.simulate_service_time('internal_services')

    def consume(self) -> None:
        """
        Consume a message from the broker.
        """
        helpers.simulate_service_time('internal_services')