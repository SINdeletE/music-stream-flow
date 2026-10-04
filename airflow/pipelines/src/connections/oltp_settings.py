from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import URL

class OltpSettings(BaseSettings):
    music_oltp_db_user: str = Field(min_length=1)
    music_oltp_db_password: SecretStr = Field(min_length=8)
    music_oltp_db_name: str = Field(min_length=1)
    music_oltp_db_host: str = Field(default="localhost", min_length=1)
    music_oltp_db_port: int = Field(ge=1, le=65535)
    music_oltp_batch_size: int = Field(ge=1)
 
    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore"
    )

    @property
    def database_url(self) -> URL:
        return URL.create(
            drivername="postgresql+asyncpg",
            username=self.music_oltp_db_user,
            password=self.music_oltp_db_password.get_secret_value(),
            host=self.music_oltp_db_host,
            port=self.music_oltp_db_port,
            database=self.music_oltp_db_name,
        )