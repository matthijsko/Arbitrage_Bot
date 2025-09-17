from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    env: str = "dev"
    redis_url: str = "redis://redis:6379/0"
    api_port: int = 8000
    opp_list_key: str = "opps:recent"
    opp_channel: str = "opportunities"
    cors_allow_origins: list[str] = ["*"]

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", env_prefix="")

settings = Settings()
