import asyncio
from src.settings import KafkaSettings 

from confluent_kafka import Consumer, TopicPartition, KafkaException

class KafkaConsumerConnector:
    def __init__(self, settings: KafkaSettings, partition: int) -> None:
        self.settings = settings
        self.partition = partition

    async def __aenter__(self) -> Consumer:
        settings = self.settings
        if settings is None:
            settings = KafkaSettings()

        self.consumer = Consumer({
            "bootstrap.servers": self.settings.kafka_bootstrap_servers_url,
            "group.id": f"{settings.music_kafka_topic_name}_{self.partition}",
            "enable.auto.commit": False
        })

        return self.consumer

    @staticmethod
    def get_current_offset(consumer: Consumer, settings: KafkaSettings, partition: int):
        tp = [TopicPartition(settings.music_kafka_topic_name, partition)]
        result = consumer.committed(tp, settings.music_kafka_timeout)

        return result[0].offset
        

    @staticmethod
    async def commit(
        consumer: Consumer,
        settings: KafkaSettings,
        partition: int,
        *,
        next_offset: int,
    ) -> None:
        partitions = await asyncio.to_thread(
            consumer.commit,
            offsets=[TopicPartition(settings.music_kafka_topic_name, partition, next_offset)],
            asynchronous=False
        )

        for result in partitions:
            if result.error is not None:
                raise KafkaException(result.error)

    async def __aexit__(self, exc_type, exc, tb) -> None:
        self.consumer.close()
