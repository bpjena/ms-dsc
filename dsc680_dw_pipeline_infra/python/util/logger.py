import logging

logging.basicConfig(
    format="{asctime} [{levelname}] ({filename}:{lineno}): {message}",
    style="{",
    level=logging.INFO,
)


def set_context(logger, context):
    _logger = logger
    while _logger:
        for handler in _logger.handlers:
            try:
                handler.set_context(context)
            except AttributeError:
                # Not all handlers need to have context passed in so we ignore
                # the error when handlers do not have set_context defined.
                pass
        if _logger.propagate is True:
            _logger = _logger.parent
        else:
            _logger = None


class LoggingMixin:
    def __init__(self, context=None):
        self._logger = None
        self._set_context(context)

    @property
    def log(self):
        """
        To remain backwards compatible
        """
        return self.logger

    @property
    def logger(self):
        if self._logger is None:
            logger = logging.getLogger(f"{self.__class__.__module__}.{self.__class__.__name__}")
            self._logger = logger
        else:
            logger = self._logger
        return logger

    def _set_context(self, context):
        if context is not None:
            set_context(self.log, context)


logger = LoggingMixin().logger
