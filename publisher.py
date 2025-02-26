import logging

from config import(
    get_connection,
    configure_logging
)

log = logging.getLogger(__name__)

def main():
    configure_logging()
    connection = get_connection()
    log.info("Created connection: %s", connection)

    while True:
        pass

if __name__ == "__main__":
    main()
