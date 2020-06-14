import asyncio

from confluent_kafka import Consumer


async def consume(topic_name):
    """Consumes data from the Kafka Topic"""
    c = Consumer({"bootstrap.servers": "PLAINTEXT://localhost:9093", "group.id": "0"})
    c.subscribe([topic_name])

    while True:
        
        messages = c.consume()

        for message in messages:
            print(f"\n{message.value()}\n")

        await asyncio.sleep(0.01)


def main():
    
    try:
        asyncio.run(consume("police.service.calls"))
    except KeyboardInterrupt as e:
        print("shutting down")


if __name__ == "__main__":
    main()