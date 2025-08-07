import subprocess
import os
from datetime import datetime
from config import Config

def backup_v1_database():
    """Create a backup of V1 database"""
    config = Config()
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_file = f"backup/v1_complete_backup_{timestamp}.sql"
    
    # Create backup directory if it doesn't exist
    os.makedirs('backup', exist_ok=True)
    
    # Build mysqldump command
    cmd = [
        'mysqldump',
        '-h', config.V1_CONFIG['host'],
        '-P', str(config.V1_CONFIG['port']),
        '-u', config.V1_CONFIG['user'],
        f'-p{config.V1_CONFIG["password"]}',
        '--single-transaction',
        '--routines',
        '--triggers',
        config.V1_CONFIG['database']
    ]
    
    print(f"Creating backup of V1 database...")
    
    try:
        with open(backup_file, 'w') as f:
            subprocess.run(cmd, stdout=f, check=True)
        
        # Check file size
        size = os.path.getsize(backup_file) / (1024 * 1024)  # MB
        print(f"✓ Backup created successfully!")
        print(f"  File: {backup_file}")
        print(f"  Size: {size:.2f} MB")
        
    except subprocess.CalledProcessError as e:
        print(f"✗ Backup failed: {e}")
        return False
    
    return True

if __name__ == "__main__":
    backup_v1_database()