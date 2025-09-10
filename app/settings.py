from pydantic_settings import BaseSettings, SettingsConfigDict


class Broker(BaseSettings):
    model_config = SettingsConfigDict(
        env_file="../.env",
        env_file_encoding="utf-8",
        env_prefix="KAFKA_",
        extra="ignore",
    )
    host: str = "127.0.0.1"
    port: int = 29092
    protocol: str = "http"


class SchemaRegister(BaseSettings):
    model_config = SettingsConfigDict(
        env_file="../.env",
        env_file_encoding="utf-8",
        env_prefix="SCHEMA_REGISTER_",
        extra="ignore",
    )
    host: str = "127.0.0.1"
    port: int = 8081
    protocol: str = "http"


broker = Broker()
schema_register = SchemaRegister()
