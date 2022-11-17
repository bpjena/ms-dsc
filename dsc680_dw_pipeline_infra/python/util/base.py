from python.util.logger import LoggingMixin


class BaseHook(LoggingMixin):
    """Base components: configuration, logging, decryption, metric"""

    def __init__(self, context=None):
        super().__init__(context)
