"""Main entry point for the application."""

import logging
import sys

from src.settings import get_settings


def setup_logging() -> None:
    """Configure basic logging for the application."""
    settings = get_settings()
    logging.basicConfig(
        level=getattr(logging, settings.log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        stream=sys.stdout,
    )


def main() -> None:
    """Run the main application."""
    setup_logging()
    logger = logging.getLogger(__name__)
    settings = get_settings()

    logger.info('Starting Python UV Simple application')
    logger.info(f'Environment: {settings.environment}')
    logger.info(f'Debug mode: {settings.debug}')
    logger.info(f'Log level: {settings.log_level}')

    try:
        # Your application logic here
        logger.info('Application running successfully')
    except Exception as e:
        logger.error(f'Application error: {e}')
        raise
    finally:
        logger.info('Application shutdown')


if __name__ == '__main__':
    main()
