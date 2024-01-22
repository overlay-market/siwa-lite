import logging


def handle_error(error_message: str, exception: Exception):
    logger = logging.getLogger(__name__)
    logger.error(f"{error_message}: {exception}")
