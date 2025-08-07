import mysql.connector
from colorama import init, Fore
import logging

init(autoreset=True)

class MigrationRollback:
    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger('MigrationRollback')
    
    def rollback(self):
        """Rollback the migration by clearing V2 table"""
        print(f"\n{Fore.RED}⚠ WARNING: This will delete all data from the V2 table!")
        response = input("Are you sure you want to rollback? (yes/no): ").lower()
        
        if response != 'yes':
            print(f"{Fore.YELLOW}Rollback cancelled")
            return
        
        try:
            v2_conn = mysql.connector.connect(**self.config.V2_CONFIG)
            v2_cursor = v2_conn.cursor()
            
            # Get count before deletion
            v2_cursor.execute(f"SELECT COUNT(*) as count FROM {self.config.V2_TABLE}")
            count = v2_cursor.fetchone()[0]
            
            print(f"\n{Fore.YELLOW}Deleting {count} records from V2...")
            
            # Delete all records
            v2_cursor.execute(f"DELETE FROM {self.config.V2_TABLE}")
            v2_conn.commit()
            
            # Reset auto-increment
            v2_cursor.execute(f"ALTER TABLE {self.config.V2_TABLE} AUTO_INCREMENT = 1")
            v2_conn.commit()
            
            print(f"{Fore.GREEN}✓ Rollback completed! Deleted {count} records")
            
            v2_cursor.close()
            v2_conn.close()
            
        except Exception as e:
            self.logger.error(f"Rollback failed: {e}")
            print(f"{Fore.RED}✗ Rollback failed: {e}")