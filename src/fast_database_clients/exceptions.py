
import logging

class ErrorException(Exception):
    """
    Base class for other exceptions
    """

    def __init__(self, message):
        self.message = message
        logging.getLogger(__name__).error(message)
        
