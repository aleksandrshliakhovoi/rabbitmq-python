import time
import logging
from typing import TYPE_CHECKING

from config import(
    get_connection,
    configure_logging,
    MQ_EXCHANGE,
    MQ_ROUTING_KEY
)

if TYPE_CHECKING:
    from pika.adapters.blocking_connection import BlockingChannel

log = logging.getLogger(__name__)

def produce_message(channel: "BlockingChannel") -> None:
    queue = channel.queue_declare(queue=MQ_ROUTING_KEY)
    log.info("Declared queue message %s %s", MQ_ROUTING_KEY, queue)
    message_body = f"Hello World! {time.time()}"
    log.info("Publish message %s", message_body) # Publish messages
    channel.basic_publish(
        exchange='',
        routing_key=MQ_EXCHANGE,
        body=MQ_ROUTING_KEY
    )
    log.warning("Published message %s", message_body)

def main():
    configure_logging(level=logging.INFO)
    with get_connection() as connection:
        log.info("Created connection: %s", connection)
        with connection.channel() as channel:
            log.info("Created channel: %s", channel)
            produce_message(channel=channel)
            produce_message(channel=channel)
            produce_message(channel=channel)

            while True:
                pass

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.warning("Bye Choomba")
