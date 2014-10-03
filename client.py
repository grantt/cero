import json
import logging
import zmq
from zmq.log.handlers import PUBHandler


class PubSubClient(object):
    """
    A simple client for pub/sub messaging using the 0MQ library.

    Args:
        host (str, optional): Hostname for this pub/sub client, defaults to 'localhost'.
        publish_port (str, optional): Port on which to publish messages, defaults to 5556.
        subscribe_port (str, optional): Port on which to receive messages, defaults to 5557.

    Attributes:
    """

    def __init__(self, host="localhost", publish_port="5556", subscribe_port="5557"):
        context = zmq.Context()

        self._pub_socket = context.socket(zmq.PUSH)
        self._pub_socket.bind("tcp://{}:{}".format(host, publish_port))

        # Add logging on the publisher
        handler = PUBHandler(self._pub_socket)
        self._logger = logging.getLogger()
        self._logger.addHandler(handler)

        self._sub_socket = context.socket(zmq.SUB)
        self._sub_socket.bind("tcp://{}:{}".format(host, subscribe_port))

    def publish(self, topic, message):
        """
        Publish a message with a

        Args:
            topic (str): The topic of the message, to be read by subscribers of that topic.
            message (str): The contents of the message to be sent

        Raises:
            TypeErrorL If 'message' is not a string
        """

        if not isinstance(message, str):
            raise TypeError("You must specify a message as a string.")

        self._pub_socket.send_string("{} {}".format(topic, message)

    def subscribe(self, topic):
        """
        Subscribe to the messages of a particular topic

        Args:
            topic (basestring): The name of the topic to subscribe to

        Returns:
            bool: True if successful

        Raises:
            TypeError: If 'topic' is not a basestring
        """

        if not isinstance(topic, basestring):
            raise TypeError("You must specify topic as a basestring")

        self._sub_socket.setsockopt(zmq.SUBSCRIBE, topic)

        return True

    def unsubscribe(self, topic):
        """
        Unsubscribe to the messages of a particular topic

        Args:
            topic (basestring): The name of the topic to subscribe to

        Returns:
            bool: True if successful

        Raises:
            TypeError: If 'topic' is not a basestring
        """

        if not isinstance(topic, basestring):
            raise TypeError("You must specify topic as a basestring")

        self._sub_socket.setsockopt(zmq.UNSUBSCRIBE, topic)

        return True

    def listen(self):
        """
        Listen continuously on the subscriber socket for incoming messages.

        Yields:
            object: The Python object representing the message received
        """

        while True:
            msg_json = self._sub_socket.recv()
            msg_obj = json.loads(msg_json)
            yield msg_obj

    def acknowledge(self, message):
        pass
