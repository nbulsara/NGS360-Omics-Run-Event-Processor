import logging
import os


def get_logger():
    ''' Sets up logging configuration '''
    VERBOSE_LOGGING = os.environ.get(
        'VERBOSE_LOGGING', 'false'
    ).lower() == 'true'
    log_level = logging.DEBUG if VERBOSE_LOGGING else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger()
    logger.setLevel(log_level)

    # Reduce boto3 logging noise
    logging.getLogger('boto3').setLevel(logging.INFO)
    logging.getLogger('botocore').setLevel(logging.INFO)
    return logger
