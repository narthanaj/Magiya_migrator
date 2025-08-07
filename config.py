import os
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()
V1_ADDRESS_TABLE = os.getenv('V1_ADDRESS_TABLE', 'addresses')
V2_ADDRESS_TABLE = os.getenv('V2_ADDRESS_TABLE', 'addresses')

class Config:
    # V1 Database Configuration
    V1_CONFIG = {
        'host': os.getenv('V1_HOST'),
        'port': int(os.getenv('V1_PORT', 3306)),
        'user': os.getenv('V1_USER'),
        'password': os.getenv('V1_PASSWORD'),
        'database': os.getenv('V1_DATABASE'),
        'raise_on_warnings': True
    }
    
    # V2 Database Configuration
    V2_CONFIG = {
        'host': os.getenv('V2_HOST'),
        'port': int(os.getenv('V2_PORT', 3306)),
        'user': os.getenv('V2_USER'),
        'password': os.getenv('V2_PASSWORD'),
        'database': os.getenv('V2_DATABASE'),
        'raise_on_warnings': True
    }
    
    # Database names (for schema queries)
    V1_DATABASE = os.getenv('V1_DATABASE')
    V2_DATABASE = os.getenv('V2_DATABASE')
    
    # Table names
    V1_TABLE = os.getenv('V1_TABLE', 'users')
    V2_TABLE = os.getenv('V2_TABLE', 'users')
    V1_ADDRESS_TABLE = V1_ADDRESS_TABLE
    V2_ADDRESS_TABLE = V2_ADDRESS_TABLE
    
    # Migration settings
    BATCH_SIZE = int(os.getenv('BATCH_SIZE', 1000))
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    
    # Default timestamp for verified emails
    DEFAULT_VERIFIED_TIMESTAMP = os.getenv('DEFAULT_VERIFIED_TIMESTAMP', 
                                          datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    
    # File paths
    LOG_FILE = f"logs/migration_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    BACKUP_FILE = f"backup/v1_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.sql"
    FAILED_RECORDS_FILE = f"logs/failed_records_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"