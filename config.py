from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    mongoURL: str = 'mongodb://localhost:27017'
    mongodb_port: int = 27017