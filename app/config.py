import os
from dotenv import load_dotenv

# Load environment variables from a .env file if present
load_dotenv()

class Config:
    """
    Configuration settings for the application.
    """
    # Environment can be 'production' or 'testing'
    ENVIRONMENT = os.getenv('APP_ENVIRONMENT', 'production').lower()

    @staticmethod
    def is_production():
        return Config.ENVIRONMENT == 'production'

    @staticmethod
    def is_testing():
        return Config.ENVIRONMENT == 'testing'
