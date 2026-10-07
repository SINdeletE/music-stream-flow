from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

class KafkaSettings:
    music_kafka_host: str = Field(min_length=1)
    music_kafka_port: str = Field(min_length=1)
    music_kafka_topic_name: str = Field(min_length=1)
    music_kafka_num_partitions: int = Field(min_length=1)
    music_kafka_batch_size: int = Field(min_length=1)
    music_kafka_timeout: float = Field(min_length=1)
    
    model_config = SettingsConfigDict(
            env_file=".env",
            extra="ignore"
        )

    @property
    def kafka_bootstrap_servers_url(self) -> str:
        return f"{self.music_kafka_host}:{self.music_kafka_port}"