import asyncio
from src.settings import KafkaSettings 

from confluent_kafka import Consumer, TopicPartition, KafkaException

class KafkaConsumerConnector:
    def __init__(self, settings: KafkaSettings, partition: int) -> None:
        self.settings = settings
        self.partition = partition

    def __aenter__(self) -> Consumer:
        settings = self.settings
        if settings is None:
            settings = KafkaSettings()

        self.consumer = Consumer({
            "bootstrap.servers": self.settings.kafka_bootstrap_servers_url,
            "group.id": f"{settings.music_kafka_topic_name}_{self.partition}",
            "enable.auto.commit": False
        })

        return self.consumer

    async def commit(self, *, next_offset: int) -> None:
        partitions = await asyncio.to_thread(
            self.consumer.commit,
            offsets=[TopicPartition(self.settings.music_kafka_topic_name, self.partition, next_offset)],
            asynchronous=False
        )

        for partition in partitions:
            if partition.error is not None:
                raise KafkaException(partition.error)

    def __aexit__(self, exc_type, exc, tb) -> None:
        pass