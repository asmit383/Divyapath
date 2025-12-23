from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    PROJECT_NAME: str = "DivyaPath Backend"
    API_V1_STR: str = "/api/v1"
    
    # Kafka
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    KAFKA_TOPIC_TRACKING: str = "volunteer_tracking"
    
    # Redis
    REDIS_URL: str = "redis://localhost:6379/0"
    
    # Database
    DATABASE_URL: str = "mysql+mysqlconnector://user:password@localhost/divyapath"

    class Config:
        case_sensitive = True

settings = Settings()
