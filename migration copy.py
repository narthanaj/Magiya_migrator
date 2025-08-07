import mysql.connector
from mysql.connector import Error
import logging
import json
from datetime import datetime
from tqdm import tqdm
from colorama import init, Fore, Style
import sys

# Initialize colorama for colored output
init(autoreset=True)

class MagiyaMigration:
    def __init__(self, config):
        self.config = config
        self.v1_conn = None
        self.v2_conn = None
        self.logger = self._setup_logger()
        self.failed_records = []
        self.stats = {
            'total_records': 0,
            'migrated_records': 0,
            'failed_records': 0,
            'warnings': []
        }
    
    def _setup_logger(self):
        """Set up logging configuration"""
        logger = logging.getLogger('MagiyaMigration')
        logger.setLevel(getattr(logging, self.config.LOG_LEVEL))
        
        # File handler
        fh = logging.FileHandler(self.config.LOG_FILE)
        fh.setLevel(logging.DEBUG)
        
        # Console handler
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        
        # Formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        
        logger.addHandler(fh)
        logger.addHandler(ch)
        
        return logger
    
    def connect_databases(self):
        """Establish connections to both databases"""
        try:
            # Connect to V1
            self.logger.info("Connecting to V1 database...")
            self.v1_conn = mysql.connector.connect(**self.config.V1_CONFIG)
            print(f"{Fore.GREEN}✓ Connected to V1 database")
            
            # Connect to V2
            self.logger.info("Connecting to V2 database...")
            self.v2_conn = mysql.connector.connect(**self.config.V2_CONFIG)
            print(f"{Fore.GREEN}✓ Connected to V2 database")
            
            return True
            
        except Error as e:
            self.logger.error(f"Database connection failed: {e}")
            print(f"{Fore.RED}✗ Database connection failed: {e}")
            return False
    
    def pre_migration_checks(self):
        """Perform pre-migration validation checks"""
        print(f"\n{Fore.CYAN}Running pre-migration checks...")
        
        v1_cursor = self.v1_conn.cursor(dictionary=True)
        
        # Check total records
        v1_cursor.execute(f"SELECT COUNT(*) as count FROM {self.config.V1_TABLE}")
        total = v1_cursor.fetchone()['count']
        self.stats['total_records'] = total
        print(f"  Total records to migrate: {Fore.YELLOW}{total}")
        
        # Check for long mobile numbers
        v1_cursor.execute(f"""
            SELECT COUNT(*) as count 
            FROM {self.config.V1_TABLE} 
            WHERE LENGTH(mobile) > 13
        """)
        long_mobile = v1_cursor.fetchone()['count']
        if long_mobile > 0:
            self.stats['warnings'].append(f"{long_mobile} mobile numbers will be truncated")
            print(f"  {Fore.YELLOW}⚠ {long_mobile} mobile numbers exceed 13 characters")
        
        # Check for empty names
        v1_cursor.execute(f"""
            SELECT COUNT(*) as count 
            FROM {self.config.V1_TABLE} 
            WHERE (firstname IS NULL OR firstname = '') 
              AND (lastname IS NULL OR lastname = '')
        """)
        empty_names = v1_cursor.fetchone()['count']
        if empty_names > 0:
            self.stats['warnings'].append(f"{empty_names} records have empty names")
            print(f"  {Fore.YELLOW}⚠ {empty_names} records have empty names")
        
        # Check for large balances
        v1_cursor.execute(f"""
            SELECT COUNT(*) as count 
            FROM {self.config.V1_TABLE} 
            WHERE balance > 9999999999.99
        """)
        large_balance = v1_cursor.fetchone()['count']
        if large_balance > 0:
            self.stats['warnings'].append(f"{large_balance} balances exceed V2 limit")
            print(f"  {Fore.YELLOW}⚠ {large_balance} balances exceed new limit")
        
        v1_cursor.close()
        
        # Ask for confirmation
        print(f"\n{Fore.CYAN}Ready to migrate {total} records.")
        response = input("Continue with migration? (yes/no): ").lower()
        return response == 'yes'
    
    def transform_record(self, record):
        """Transform a V1 record to V2 format"""
        try:
            # Combine names with proper spacing
            firstname = (record.get('firstname') or '').strip()
            lastname = (record.get('lastname') or '').strip()
            
            if firstname and lastname:
                name = f"{firstname} {lastname}"
            else:
                name = firstname or lastname or f"User_{record['id']}"
            
            # Convert email verified flag to timestamp
            email_verified_at = None
            if record.get('ev') == 1:
                email_verified_at = datetime.strptime(
                    self.config.DEFAULT_VERIFIED_TIMESTAMP, 
                    '%Y-%m-%d %H:%M:%S'
                )
            
            # Handle mobile truncation
            mobile = None
            if record.get('mobile'):
                mobile = str(record['mobile'])[:13]
                if len(str(record['mobile'])) > 13:
                    self.logger.warning(
                        f"Mobile truncated for record {record['id']}: "
                        f"{record['mobile']} -> {mobile}"
                    )
            
            # Round balance
            balance = round(float(record.get('balance', 0)), 2)
            
            return {
                'id': record['id'],
                'operator_id': None,
                'name': name,
                'email': record.get('email'),
                'email_verified_at': email_verified_at,
                'password': record.get('password'),
                'two_factor_secret': None,
                'two_factor_recovery_codes': None,
                'two_factor_confirmed_at': None,
                'mobile': mobile,
                'gender': record.get('gender'),
                'city_id': record.get('city_id'),
                'address': record.get('address'),
                'privacy_policy': None,
                'terms_of_service': None,
                'postal_code': None,
                'balance': balance,
                'remember_token': record.get('remember_token'),
                'current_team_id': None,
                'profile_photo_path': None,
                'keycard': record.get('rfid_key'),
                'otp': record.get('ver_code'),
                'otp_generated_at': record.get('ver_code_send_at'),
                'public': record.get('public', 0),
                'status': record.get('status', 1),
                'created_by': None,
                'updated_by': None,
                'created_at': record.get('created_at'),
                'updated_at': record.get('updated_at'),
                'otp_verified': 0
            }
            
        except Exception as e:
            self.logger.error(f"Error transforming record {record['id']}: {e}")
            raise
    
    def migrate_batch(self, records):
        """Migrate a batch of records"""
        v2_cursor = self.v2_conn.cursor()
        
        insert_query = f"""
        INSERT INTO {self.config.V2_TABLE} (
            id, operator_id, name, email, email_verified_at, password,
            two_factor_secret, two_factor_recovery_codes, two_factor_confirmed_at,
            mobile, gender, city_id, address, privacy_policy, terms_of_service,
            postal_code, balance, remember_token, current_team_id, 
            profile_photo_path, keycard, otp, otp_generated_at, public, 
            status, created_by, updated_by, created_at, updated_at, otp_verified
        ) VALUES (
            %(id)s, %(operator_id)s, %(name)s, %(email)s, %(email_verified_at)s,
            %(password)s, %(two_factor_secret)s, %(two_factor_recovery_codes)s,
            %(two_factor_confirmed_at)s, %(mobile)s, %(gender)s, %(city_id)s,
            %(address)s, %(privacy_policy)s, %(terms_of_service)s, %(postal_code)s,
            %(balance)s, %(remember_token)s, %(current_team_id)s,
            %(profile_photo_path)s, %(keycard)s, %(otp)s, %(otp_generated_at)s,
            %(public)s, %(status)s, %(created_by)s, %(updated_by)s,
            %(created_at)s, %(updated_at)s, %(otp_verified)s
        )
        """
        
        success_count = 0
        for record in records:
            try:
                transformed = self.transform_record(record)
                v2_cursor.execute(insert_query, transformed)
                success_count += 1
                self.stats['migrated_records'] += 1
            except Exception as e:
                self.logger.error(f"Failed to migrate record {record['id']}: {e}")
                self.failed_records.append({
                    'record': record,
                    'error': str(e)
                })
                self.stats['failed_records'] += 1
        
        self.v2_conn.commit()
        v2_cursor.close()
        
        return success_count
    
    def migrate(self):
        """Main migration process"""
        print(f"\n{Fore.CYAN}Starting migration...")
        
        v1_cursor = self.v1_conn.cursor(dictionary=True)
        
        # Create progress bar
        progress_bar = tqdm(
            total=self.stats['total_records'],
            desc="Migrating records",
            unit="records"
        )
        
        offset = 0
        while True:
            # Fetch batch
            query = f"""
                SELECT * FROM {self.config.V1_TABLE}
                ORDER BY id
                LIMIT {self.config.BATCH_SIZE} OFFSET {offset}
            """
            
            v1_cursor.execute(query)
            records = v1_cursor.fetchall()
            
            if not records:
                break
            
            # Migrate batch
            migrated = self.migrate_batch(records)
            progress_bar.update(migrated)
            
            offset += self.config.BATCH_SIZE
        
        progress_bar.close()
        v1_cursor.close()
        
        # Save failed records if any
        if self.failed_records:
            with open(self.config.FAILED_RECORDS_FILE, 'w') as f:
                json.dump(self.failed_records, f, indent=2, default=str)
            print(f"\n{Fore.YELLOW}Failed records saved to: {self.config.FAILED_RECORDS_FILE}")
    
    def post_migration_validation(self):
        """Validate migration results"""
        print(f"\n{Fore.CYAN}Running post-migration validation...")
        
        v2_cursor = self.v2_conn.cursor(dictionary=True)
        
        # Check record count
        v2_cursor.execute(f"SELECT COUNT(*) as count FROM {self.config.V2_TABLE}")
        v2_count = v2_cursor.fetchone()['count']
        
        print(f"\n{Fore.CYAN}Migration Summary:")
        print(f"  Total V1 records: {self.stats['total_records']}")
        print(f"  Successfully migrated: {Fore.GREEN}{self.stats['migrated_records']}")
        print(f"  Failed records: {Fore.RED}{self.stats['failed_records']}")
        print(f"  V2 table count: {v2_count}")
        
        # Check for empty names
        v2_cursor.execute(f"""
            SELECT COUNT(*) as count 
            FROM {self.config.V2_TABLE} 
            WHERE name IS NULL OR name = ''
        """)
        empty_names = v2_cursor.fetchone()['count']
        if empty_names > 0:
            print(f"  {Fore.YELLOW}⚠ {empty_names} records have empty names in V2")
        
        # Sample data
        print(f"\n{Fore.CYAN}Sample migrated data:")
        v2_cursor.execute(f"""
            SELECT id, name, email, mobile, balance 
            FROM {self.config.V2_TABLE} 
            LIMIT 5
        """)
        
        for record in v2_cursor.fetchall():
            print(f"  ID: {record['id']}, Name: {record['name']}, "
                  f"Email: {record['email']}, Mobile: {record['mobile']}, "
                  f"Balance: {record['balance']}")
        
        v2_cursor.close()
        
        # Display warnings
        if self.stats['warnings']:
            print(f"\n{Fore.YELLOW}Warnings:")
            for warning in self.stats['warnings']:
                print(f"  ⚠ {warning}")
        
        # Final status
        if self.stats['failed_records'] == 0:
            print(f"\n{Fore.GREEN}✓ Migration completed successfully!")
        else:
            print(f"\n{Fore.YELLOW}⚠ Migration completed with {self.stats['failed_records']} failures")
    
    def close_connections(self):
        """Close database connections"""
        if self.v1_conn and self.v1_conn.is_connected():
            self.v1_conn.close()
            self.logger.info("V1 connection closed")
        
        if self.v2_conn and self.v2_conn.is_connected():
            self.v2_conn.close()
            self.logger.info("V2 connection closed")
    
    def run(self):
        """Execute the complete migration process"""
        try:
            # Connect to databases
            if not self.connect_databases():
                return
            
            # Pre-migration checks
            if not self.pre_migration_checks():
                print(f"{Fore.YELLOW}Migration cancelled by user")
                return
            
            # Run migration
            self.migrate()
            
            # Post-migration validation
            self.post_migration_validation()
            
        except Exception as e:
            self.logger.error(f"Migration failed: {e}")
            print(f"{Fore.RED}✗ Migration failed: {e}")
            
        finally:
            self.close_connections()