import mysql.connector
from mysql.connector import Error
import logging
import json
from datetime import datetime
from tqdm import tqdm
from colorama import init, Fore, Style
import sys
from collections import defaultdict

init(autoreset=True)

class MagiyaMigration:
    def __init__(self, config):
        self.config = config
        self.v1_conn = None
        self.v2_conn = None
        self.logger = self._setup_logger()
        self.failed_records = []
        self.duplicate_emails = defaultdict(list)
        self.duplicate_mobiles = defaultdict(list)
        self.stats = {
            'total_records': 0,
            'migrated_records': 0,
            'skipped_records': 0,
            'updated_records': 0,
            'failed_records': 0,
            'duplicate_key_errors': 0,
            'duplicate_email_errors': 0,
            'duplicate_mobile_errors': 0,
            'warnings': []
        }
        self.migration_mode = 'insert'  # 'insert', 'update', 'upsert'
    
    def _setup_logger(self):
        """Set up logging configuration"""
        logger = logging.getLogger('MagiyaMigration')
        logger.setLevel(getattr(logging, self.config.LOG_LEVEL))
        
        fh = logging.FileHandler(self.config.LOG_FILE)
        fh.setLevel(logging.DEBUG)
        
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        
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
            self.logger.info("Connecting to V1 database...")
            self.v1_conn = mysql.connector.connect(**self.config.V1_CONFIG)
            print(f"{Fore.GREEN}✓ Connected to V1 database")
            
            self.logger.info("Connecting to V2 database...")
            self.v2_conn = mysql.connector.connect(**self.config.V2_CONFIG)
            print(f"{Fore.GREEN}✓ Connected to V2 database")
            
            return True
            
        except Error as e:
            self.logger.error(f"Database connection failed: {e}")
            print(f"{Fore.RED}✗ Database connection failed: {e}")
            return False
    
    def analyze_existing_data(self):
        """Analyze existing data in V2 table"""
        v2_cursor = self.v2_conn.cursor(dictionary=True)
        
        # Check existing records
        v2_cursor.execute(f"SELECT COUNT(*) as count FROM {self.config.V2_TABLE}")
        existing_count = v2_cursor.fetchone()['count']
        
        if existing_count > 0:
            print(f"\n{Fore.YELLOW}⚠ V2 table already contains {existing_count} records")
            
            # Get ID range
            v2_cursor.execute(f"""
                SELECT MIN(id) as min_id, MAX(id) as max_id 
                FROM {self.config.V2_TABLE}
            """)
            id_range = v2_cursor.fetchone()
            print(f"  Existing ID range: {id_range['min_id']} - {id_range['max_id']}")
            
            # Check for overlapping IDs
            v1_cursor = self.v1_conn.cursor(dictionary=True)
            v1_cursor.execute(f"""
                SELECT COUNT(*) as count 
                FROM {self.config.V1_TABLE} 
                WHERE id <= {id_range['max_id']}
            """)
            overlap_count = v1_cursor.fetchone()['count']
            
            if overlap_count > 0:
                print(f"  {Fore.YELLOW}⚠ {overlap_count} V1 records have IDs that already exist in V2")
            
            v1_cursor.close()
        
        v2_cursor.close()
        return existing_count
    
    def select_migration_mode(self):
        """Let user select migration mode"""
        print(f"\n{Fore.CYAN}Select migration mode:")
        print("1. Skip existing records (INSERT IGNORE)")
        print("2. Update existing records (INSERT ... ON DUPLICATE KEY UPDATE)")
        print("3. Clear V2 table and migrate fresh")
        print("4. Migrate only new records (WHERE id > max_v2_id)")
        print("5. Cancel migration")
        
        choice = input("\nEnter your choice (1-5): ")
        
        if choice == '1':
            self.migration_mode = 'skip'
            return True
        elif choice == '2':
            self.migration_mode = 'upsert'
            return True
        elif choice == '3':
            confirm = input(f"\n{Fore.RED}⚠ This will DELETE all existing V2 data. Are you sure? (yes/no): ")
            if confirm.lower() == 'yes':
                self.clear_v2_table()
                self.migration_mode = 'insert'
                return True
        elif choice == '4':
            self.migration_mode = 'incremental'
            return True
        elif choice == '5':
            return False
        else:
            print(f"{Fore.RED}Invalid choice")
            return self.select_migration_mode()
    
    def clear_v2_table(self):
        """Clear V2 table"""
        v2_cursor = self.v2_conn.cursor()
        v2_cursor.execute(f"DELETE FROM {self.config.V2_TABLE}")
        v2_cursor.execute(f"ALTER TABLE {self.config.V2_TABLE} AUTO_INCREMENT = 1")
        self.v2_conn.commit()
        v2_cursor.close()
        print(f"{Fore.GREEN}✓ V2 table cleared")
    
    def pre_migration_checks(self):
        """Perform pre-migration validation checks"""
        print(f"\n{Fore.CYAN}Running pre-migration checks...")
        
        v1_cursor = self.v1_conn.cursor(dictionary=True)
        
        # Check total records
        v1_cursor.execute(f"SELECT COUNT(*) as count FROM {self.config.V1_TABLE}")
        total = v1_cursor.fetchone()['count']
        self.stats['total_records'] = total
        print(f"  Total records to migrate: {Fore.YELLOW}{total}")
        
        # Check for duplicate emails in V1
        v1_cursor.execute(f"""
            SELECT email, COUNT(*) as count 
            FROM {self.config.V1_TABLE} 
            WHERE email IS NOT NULL AND email != ''
            GROUP BY email 
            HAVING count > 1
        """)
        dup_emails = v1_cursor.fetchall()
        if dup_emails:
            print(f"  {Fore.YELLOW}⚠ Found {len(dup_emails)} duplicate emails in V1")
            for dup in dup_emails[:5]:  # Show first 5
                print(f"    - {dup['email']} ({dup['count']} occurrences)")
        
        # Check for duplicate mobiles in V1
        v1_cursor.execute(f"""
            SELECT mobile, COUNT(*) as count 
            FROM {self.config.V1_TABLE} 
            WHERE mobile IS NOT NULL AND mobile != ''
            GROUP BY mobile 
            HAVING count > 1
        """)
        dup_mobiles = v1_cursor.fetchall()
        if dup_mobiles:
            print(f"  {Fore.YELLOW}⚠ Found {len(dup_mobiles)} duplicate mobile numbers in V1")
            for dup in dup_mobiles[:5]:  # Show first 5
                print(f"    - {dup['mobile']} ({dup['count']} occurrences)")
        
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
        
        v1_cursor.close()
        
        # Check existing V2 data
        existing_count = self.analyze_existing_data()
        
        if existing_count > 0:
            if not self.select_migration_mode():
                return False
        
        # Ask for confirmation
        print(f"\n{Fore.CYAN}Ready to migrate {total} records.")
        response = input("Continue with migration? (yes/no): ").lower()
        return response == 'yes'
    
    def get_max_v2_id(self):
        """Get maximum ID from V2 table"""
        v2_cursor = self.v2_conn.cursor()
        v2_cursor.execute(f"SELECT COALESCE(MAX(id), 0) as max_id FROM {self.config.V2_TABLE}")
        max_id = v2_cursor.fetchone()[0]
        v2_cursor.close()
        return max_id
    
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
    
    def build_migration_query(self):
        """Build the appropriate migration query based on mode"""
        base_columns = """
            id, operator_id, name, email, email_verified_at, password,
            two_factor_secret, two_factor_recovery_codes, two_factor_confirmed_at,
            mobile, gender, city_id, address, privacy_policy, terms_of_service,
            postal_code, balance, remember_token, current_team_id, 
            profile_photo_path, keycard, otp, otp_generated_at, public, 
            status, created_by, updated_by, created_at, updated_at, otp_verified
        """
        
        value_placeholders = """
            %(id)s, %(operator_id)s, %(name)s, %(email)s, %(email_verified_at)s,
            %(password)s, %(two_factor_secret)s, %(two_factor_recovery_codes)s,
            %(two_factor_confirmed_at)s, %(mobile)s, %(gender)s, %(city_id)s,
            %(address)s, %(privacy_policy)s, %(terms_of_service)s, %(postal_code)s,
            %(balance)s, %(remember_token)s, %(current_team_id)s,
            %(profile_photo_path)s, %(keycard)s, %(otp)s, %(otp_generated_at)s,
            %(public)s, %(status)s, %(created_by)s, %(updated_by)s,
            %(created_at)s, %(updated_at)s, %(otp_verified)s
        """
        
        if self.migration_mode == 'skip':
            return f"""
                INSERT IGNORE INTO {self.config.V2_TABLE} ({base_columns})
                VALUES ({value_placeholders})
            """
        elif self.migration_mode == 'upsert':
            return f"""
                INSERT INTO {self.config.V2_TABLE} ({base_columns})
                VALUES ({value_placeholders})
                ON DUPLICATE KEY UPDATE
                    name = VALUES(name),
                    email = VALUES(email),
                    email_verified_at = VALUES(email_verified_at),
                    password = VALUES(password),
                    mobile = VALUES(mobile),
                    gender = VALUES(gender),
                    city_id = VALUES(city_id),
                    address = VALUES(address),
                    balance = VALUES(balance),
                    keycard = VALUES(keycard),
                    otp = VALUES(otp),
                    otp_generated_at = VALUES(otp_generated_at),
                    public = VALUES(public),
                    status = VALUES(status),
                    updated_at = VALUES(updated_at)
            """
        else:
            return f"""
                INSERT INTO {self.config.V2_TABLE} ({base_columns})
                VALUES ({value_placeholders})
            """
    
    def migrate_batch(self, records):
        """Migrate a batch of records"""
        v2_cursor = self.v2_conn.cursor()
        
        insert_query = self.build_migration_query()
        
        success_count = 0
        for record in records:
            try:
                transformed = self.transform_record(record)
                v2_cursor.execute(insert_query, transformed)
                
                if self.migration_mode == 'upsert' and v2_cursor.rowcount == 2:
                    self.stats['updated_records'] += 1
                else:
                    self.stats['migrated_records'] += 1
                
                success_count += 1
                
            except mysql.connector.IntegrityError as e:
                error_msg = str(e)
                
                if "Duplicate entry" in error_msg and "PRIMARY" in error_msg:
                    self.stats['duplicate_key_errors'] += 1
                    if self.migration_mode != 'skip':
                        self.logger.error(f"Duplicate primary key for record {record['id']}: {e}")
                    else:
                        self.stats['skipped_records'] += 1
                        
                elif "Duplicate entry" in error_msg and "email_unique" in error_msg:
                    self.stats['duplicate_email_errors'] += 1
                    self.duplicate_emails[record.get('email')].append(record['id'])
                    self.logger.error(f"Duplicate email for record {record['id']}: {e}")
                    
                elif "Duplicate entry" in error_msg and "mobile_unique" in error_msg:
                    self.stats['duplicate_mobile_errors'] += 1
                    self.duplicate_mobiles[record.get('mobile')].append(record['id'])
                    self.logger.error(f"Duplicate mobile for record {record['id']}: {e}")
                    
                else:
                    self.logger.error(f"Integrity error for record {record['id']}: {e}")
                
                self.failed_records.append({
                    'record': record,
                    'error': str(e),
                    'error_type': 'IntegrityError'
                })
                self.stats['failed_records'] += 1
                
            except Exception as e:
                self.logger.error(f"Failed to migrate record {record['id']}: {e}")
                self.failed_records.append({
                    'record': record,
                    'error': str(e),
                    'error_type': type(e).__name__
                })
                self.stats['failed_records'] += 1
        
        self.v2_conn.commit()
        v2_cursor.close()
        
        return success_count
    
    def migrate(self):
        """Main migration process"""
        print(f"\n{Fore.CYAN}Starting migration...")
        
        v1_cursor = self.v1_conn.cursor(dictionary=True)
        
        # Build query based on mode
        if self.migration_mode == 'incremental':
            max_v2_id = self.get_max_v2_id()
            print(f"  Migrating records with ID > {max_v2_id}")
            base_query = f"""
                SELECT * FROM {self.config.V1_TABLE}
                WHERE id > {max_v2_id}
                ORDER BY id
            """
        else:
            base_query = f"""
                SELECT * FROM {self.config.V1_TABLE}
                ORDER BY id
            """
        
        # Get total count for progress bar
        if self.migration_mode == 'incremental':
            count_query = f"""
                SELECT COUNT(*) as count FROM {self.config.V1_TABLE}
                WHERE id > {max_v2_id}
            """
            v1_cursor.execute(count_query)
            total_to_migrate = v1_cursor.fetchone()['count']
        else:
            total_to_migrate = self.stats['total_records']
        
        # Create progress bar
        progress_bar = tqdm(
            total=total_to_migrate,
            desc="Migrating records",
            unit="records"
        )
        
        offset = 0
        while True:
            # Fetch batch
            query = f"{base_query} LIMIT {self.config.BATCH_SIZE} OFFSET {offset}"
            
            v1_cursor.execute(query)
            records = v1_cursor.fetchall()
            
            if not records:
                break
            
            # Migrate batch
            migrated = self.migrate_batch(records)
            progress_bar.update(len(records))
            
            offset += self.config.BATCH_SIZE
        
        progress_bar.close()
        v1_cursor.close()
        
        # Save failed records and analysis
        if self.failed_records:
            self.save_migration_report()
    
    def save_migration_report(self):
        """Save detailed migration report"""
        report = {
            'summary': self.stats,
            'failed_records': self.failed_records,
            'duplicate_emails': dict(self.duplicate_emails),
            'duplicate_mobiles': dict(self.duplicate_mobiles),
            'migration_mode': self.migration_mode,
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        report_file = self.config.FAILED_RECORDS_FILE
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        print(f"\n{Fore.YELLOW}Migration report saved to: {report_file}")
    
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
        if self.stats['updated_records'] > 0:
            print(f"  Updated existing: {Fore.BLUE}{self.stats['updated_records']}")
        if self.stats['skipped_records'] > 0:
            print(f"  Skipped existing: {Fore.YELLOW}{self.stats['skipped_records']}")
        print(f"  Failed records: {Fore.RED}{self.stats['failed_records']}")
        
        if self.stats['duplicate_key_errors'] > 0:
            print(f"    - Duplicate key errors: {self.stats['duplicate_key_errors']}")
        if self.stats['duplicate_email_errors'] > 0:
            print(f"    - Duplicate email errors: {self.stats['duplicate_email_errors']}")
        if self.stats['duplicate_mobile_errors'] > 0:
            print(f"    - Duplicate mobile errors: {self.stats['duplicate_mobile_errors']}")
        
        print(f"  V2 table total count: {v2_count}")
        
        # Show duplicate analysis
        if self.duplicate_emails:
            print(f"\n{Fore.YELLOW}Top duplicate emails:")
            sorted_emails = sorted(self.duplicate_emails.items(), 
                                 key=lambda x: len(x[1]), reverse=True)[:5]
            for email, ids in sorted_emails:
                print(f"  - {email}: IDs {ids[:5]}{'...' if len(ids) > 5 else ''}")
        
        if self.duplicate_mobiles:
            print(f"\n{Fore.YELLOW}Top duplicate mobile numbers:")
            sorted_mobiles = sorted(self.duplicate_mobiles.items(), 
                                  key=lambda x: len(x[1]), reverse=True)[:5]
            for mobile, ids in sorted_mobiles:
                print(f"  - {mobile}: IDs {ids[:5]}{'...' if len(ids) > 5 else ''}")
        
        # Sample data
        print(f"\n{Fore.CYAN}Sample migrated data:")
        v2_cursor.execute(f"""
            SELECT id, name, email, mobile, balance 
            FROM {self.config.V2_TABLE} 
            ORDER BY id DESC
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
            print(f"  Review the migration report for details")
    
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