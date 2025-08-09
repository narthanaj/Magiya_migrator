import mysql.connector
from mysql.connector import Error
import logging
import json
from datetime import datetime
from tqdm import tqdm
from colorama import init, Fore, Style
import sys
from collections import defaultdict
import re

init(autoreset=True)

class MagiyaMigration:
    def __init__(self, config):
        self.config = config
        self.v1_conn = None
        self.v2_conn = None
        self.logger = self._setup_logger()
        self.failed_records = {'users': [], 'addresses': []}
        self.duplicate_emails = defaultdict(list)
        self.duplicate_mobiles = defaultdict(list)
        self.id_mapping = {'users': {}, 'addresses': {}}  # Maps V1 IDs to V2 IDs
        self.stats = {
            'users': {
                'total_records': 0,
                'migrated_records': 0,
                'skipped_records': 0,
                'skipped_status_zero': 0,
                'updated_records': 0,
                'failed_records': 0,
                'duplicate_key_errors': 0,
                'duplicate_email_errors': 0,
                'duplicate_mobile_errors': 0,
                'mobile_conversions': 0,
                'mobile_null_or_empty': 0,
                'mobile_invalid': 0,
                'role_assignments_success': 0,
                'role_assignments_failed': 0,
                'gender_conversions': {
                    'M_to_Male': 0, 
                    'F_to_Female': 0, 
                    'null': 0, 
                    'empty': 0,
                    'unchanged': 0,
                    'other_values': defaultdict(int)
                },
                'warnings': []
            },
            'addresses': {
                'total_records': 0,
                'migrated_records': 0,
                'skipped_records': 0,
                'updated_records': 0,
                'failed_records': 0,
                'duplicate_key_errors': 0,
                'warnings': []
            }
        }
        self.migration_mode = 'insert'
        self.preserve_ids = True
        self.migrate_addresses = False
    
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
    
    def convert_mobile_number(self, mobile):
        """Convert mobile number to +94 format"""
        if mobile is None:
            self.stats['users']['mobile_null_or_empty'] += 1
            return None
            
        mobile = str(mobile).strip()
        
        if not mobile or mobile == '':
            self.stats['users']['mobile_null_or_empty'] += 1
            return None
        
        mobile = re.sub(r'[\s\-\(\)]', '', mobile)
        
        if mobile.startswith('+94') and len(mobile) == 12:
            return mobile
        
        mobile = re.sub(r'^\+94', '', mobile)
        mobile = re.sub(r'^0094', '', mobile)
        mobile = re.sub(r'^94', '', mobile)
        
        if mobile.startswith('0'):
            mobile = mobile[1:]
        
        if not mobile or not mobile.isdigit():
            self.stats['users']['mobile_invalid'] += 1
            self.logger.warning(f"Invalid mobile number format: {mobile}")
            return None
        
        if len(mobile) == 9:
            self.stats['users']['mobile_conversions'] += 1
            return f"+94{mobile}"
        else:
            self.stats['users']['mobile_invalid'] += 1
            self.logger.warning(f"Invalid mobile number length: {mobile} (length: {len(mobile)})")
            return None
    
    def convert_gender(self, gender):
        """Convert gender values M/F to Male/Female"""
        if gender is None:
            self.stats['users']['gender_conversions']['null'] += 1
            return None
        
        gender_str = str(gender).strip()
        
        if gender_str == '' or gender_str.lower() == 'none':
            self.stats['users']['gender_conversions']['empty'] += 1
            return None
        
        gender_upper = gender_str.upper()
        
        if gender_upper == 'M':
            self.stats['users']['gender_conversions']['M_to_Male'] += 1
            return 'Male'
        elif gender_upper == 'F':
            self.stats['users']['gender_conversions']['F_to_Female'] += 1
            return 'Female'
        elif gender_upper == 'MALE':
            self.stats['users']['gender_conversions']['unchanged'] += 1
            return 'Male'
        elif gender_upper == 'FEMALE':
            self.stats['users']['gender_conversions']['unchanged'] += 1
            return 'Female'
        else:
            self.stats['users']['gender_conversions']['other_values'][gender_str] += 1
            self.logger.warning(f"Unknown gender value: {gender_str}")
            return gender_str
    
    def check_address_table_exists(self):
        """Check if address table exists in V1"""
        v1_cursor = self.v1_conn.cursor()
        try:
            v1_cursor.execute(f"""
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_schema = %s 
                AND table_name = %s
            """, (self.config.V1_DATABASE, self.config.V1_ADDRESS_TABLE))
            
            exists = v1_cursor.fetchone()[0] > 0
            
            if exists:
                v1_cursor.execute(f"SELECT COUNT(*) FROM {self.config.V1_ADDRESS_TABLE}")
                count = v1_cursor.fetchone()[0]
                print(f"{Fore.CYAN}Found address table with {count} records")
                return True
            else:
                print(f"{Fore.YELLOW}Address table '{self.config.V1_ADDRESS_TABLE}' not found in V1")
                return False
                
        except Exception as e:
            self.logger.error(f"Error checking address table: {e}")
            return False
        finally:
            v1_cursor.close()
    
    def check_role_user_table_exists(self):
        """Check if role_user table exists in V2"""
        v2_cursor = self.v2_conn.cursor()
        try:
            v2_cursor.execute(f"""
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_schema = %s 
                AND table_name = 'role_user'
            """, (self.config.V2_DATABASE,))
            
            exists = v2_cursor.fetchone()[0] > 0
            
            if exists:
                print(f"{Fore.CYAN}Found role_user table for role assignments")
                return self.verify_role_user_table_structure()
            else:
                print(f"{Fore.YELLOW}⚠ role_user table not found - role assignments will be skipped")
                return False
                
        except Exception as e:
            self.logger.error(f"Error checking role_user table: {e}")
            return False
        finally:
            v2_cursor.close()
    
    def select_tables_to_migrate(self):
        """Let user select which tables to migrate"""
        print(f"\n{Fore.CYAN}Select tables to migrate:")
        
        has_address_table = self.check_address_table_exists()
        self.has_role_user_table = self.check_role_user_table_exists()
        
        if has_address_table:
            print("1. Users table only")
            print("2. Address table only")
            print("3. Both users and address tables")
            print("4. Cancel migration")
            
            choice = input("\nEnter your choice (1-4): ")
            
            if choice == '1':
                self.migrate_addresses = False
                return True
            elif choice == '2':
                self.migrate_addresses = True
                self.migrate_users = False
                return True
            elif choice == '3':
                self.migrate_addresses = True
                self.migrate_users = True
                return True
            elif choice == '4':
                return False
            else:
                print(f"{Fore.RED}Invalid choice")
                return self.select_tables_to_migrate()
        else:
            self.migrate_addresses = False
            self.migrate_users = True
            print(f"{Fore.CYAN}Will migrate users table only")
            return True
    
    def analyze_existing_data(self):
        """Analyze existing data in V2 tables"""
        v2_cursor = self.v2_conn.cursor(dictionary=True)
        results = {}
        
        v2_cursor.execute(f"SELECT COUNT(*) as count FROM {self.config.V2_TABLE}")
        users_count = v2_cursor.fetchone()['count']
        
        if users_count > 0:
            print(f"\n{Fore.YELLOW}⚠ V2 users table already contains {users_count} records")
            v2_cursor.execute(f"SELECT MIN(id) as min_id, MAX(id) as max_id FROM {self.config.V2_TABLE}")
            id_range = v2_cursor.fetchone()
            print(f"  Existing ID range: {id_range['min_id']} - {id_range['max_id']}")
            results['users'] = users_count
        else:
            results['users'] = 0
        
        if self.migrate_addresses:
            try:
                v2_cursor.execute(f"SELECT COUNT(*) as count FROM {self.config.V2_ADDRESS_TABLE}")
                address_count = v2_cursor.fetchone()['count']
                if address_count > 0:
                    print(f"\n{Fore.YELLOW}⚠ V2 address table already contains {address_count} records")
                results['addresses'] = address_count
            except:
                results['addresses'] = 0
        
        v2_cursor.close()
        return results
    
    def select_migration_strategy(self, has_existing_data=False):
        """Let user select migration strategy"""
        print(f"\n{Fore.CYAN}Select migration strategy:")
        
        if has_existing_data:
            print("1. Skip existing records (INSERT IGNORE)")
            print("2. Update existing records (INSERT ... ON DUPLICATE KEY UPDATE)")
            print("3. Clear V2 table(s) and migrate fresh")
            print("4. Cancel migration")
            
            choice = input("\nEnter your choice (1-4): ")
            
            if choice == '1':
                self.migration_mode = 'skip'
                return self.select_id_strategy()
            elif choice == '2':
                self.migration_mode = 'upsert'
                return self.select_id_strategy()
            elif choice == '3':
                confirm = input(f"\n{Fore.RED}⚠ This will DELETE all existing V2 data. Are you sure? (yes/no): ")
                if confirm.lower() == 'yes':
                    self.clear_v2_tables()
                    self.migration_mode = 'insert'
                    return self.select_id_strategy()
                else:
                    return False
            elif choice == '4':
                return False
        else:
            self.migration_mode = 'insert'
            return self.select_id_strategy()
    
    def select_id_strategy(self):
        """Select ID handling strategy"""
        print(f"\n{Fore.CYAN}Select ID handling strategy:")
        print("1. Auto-increment new IDs (recommended)")
        print("2. Preserve original IDs from V1")
        
        choice = input("\nEnter your choice (1-2): ")
        
        if choice == '1':
            self.preserve_ids = False
            return True
        elif choice == '2':
            self.preserve_ids = True
            return True
        else:
            print(f"{Fore.RED}Invalid choice")
            return self.select_id_strategy()
    
    def clear_v2_tables(self):
        """Clear V2 tables"""
        v2_cursor = self.v2_conn.cursor()
        
        if hasattr(self, 'migrate_users') and self.migrate_users:
            v2_cursor.execute(f"DELETE FROM {self.config.V2_TABLE}")
            v2_cursor.execute(f"ALTER TABLE {self.config.V2_TABLE} AUTO_INCREMENT = 1")
            print(f"{Fore.GREEN}✓ V2 users table cleared")
        
        if self.migrate_addresses:
            try:
                v2_cursor.execute(f"DELETE FROM {self.config.V2_ADDRESS_TABLE}")
                v2_cursor.execute(f"ALTER TABLE {self.config.V2_ADDRESS_TABLE} AUTO_INCREMENT = 1")
                print(f"{Fore.GREEN}✓ V2 address table cleared")
            except Exception as e:
                self.logger.warning(f"Could not clear address table: {e}")
        
        self.v2_conn.commit()
        v2_cursor.close()
    
    def pre_migration_checks(self):
        """Perform pre-migration validation checks"""
        print(f"\n{Fore.CYAN}Running pre-migration checks...")
        
        if not self.select_tables_to_migrate():
            return False
        
        v1_cursor = self.v1_conn.cursor(dictionary=True)
        
        if hasattr(self, 'migrate_users') and self.migrate_users:
            v1_cursor.execute(f"SELECT COUNT(*) as count FROM {self.config.V1_TABLE}")
            total = v1_cursor.fetchone()['count']
            self.stats['users']['total_records'] = total
            print(f"\n{Fore.CYAN}Users table:")
            print(f"  Total records in V1: {Fore.YELLOW}{total}")
            
            v1_cursor.execute(f"SELECT COUNT(*) as count FROM {self.config.V1_TABLE} WHERE status = 0")
            status_zero_count = v1_cursor.fetchone()['count']
            if status_zero_count > 0:
                print(f"  {Fore.YELLOW}⚠ {status_zero_count} records with status=0 will be skipped")
            
            actual_migration_count = total - status_zero_count
            print(f"  Records to actually migrate: {Fore.GREEN}{actual_migration_count}")
            
            v1_cursor.execute(f"""
                SELECT COUNT(*) as count FROM {self.config.V1_TABLE} 
                WHERE mobile IS NOT NULL AND mobile != '' AND mobile NOT LIKE '+94%' AND status != 0
            """)
            mobile_converts = v1_cursor.fetchone()['count']
            if mobile_converts > 0:
                print(f"  {Fore.CYAN}ℹ {mobile_converts} mobile numbers will be converted to +94 format")
            
            v1_cursor.execute(f"""
                SELECT CASE WHEN gender IS NULL THEN 'NULL' WHEN gender = '' THEN 'EMPTY' ELSE gender END as gender_value,
                COUNT(*) as count FROM {self.config.V1_TABLE} WHERE status != 0 GROUP BY gender_value ORDER BY count DESC
            """)
            gender_stats = v1_cursor.fetchall()
            
            print(f"  {Fore.CYAN}Gender distribution (excluding status=0):")
            for stat in gender_stats:
                print(f"    - {stat['gender_value']}: {stat['count']} records")
            
            if self.has_role_user_table:
                print(f"  {Fore.CYAN}ℹ Users will be assigned role_id=10 in role_user table")
            else:
                print(f"  {Fore.YELLOW}⚠ role_user table not found - no role assignments will be made")
        
        if self.migrate_addresses:
            v1_cursor.execute(f"SELECT COUNT(*) as count FROM {self.config.V1_ADDRESS_TABLE}")
            total_addresses = v1_cursor.fetchone()['count']
            self.stats['addresses']['total_records'] = total_addresses
            print(f"\n{Fore.CYAN}Address table:")
            print(f"  Total records to migrate: {Fore.YELLOW}{total_addresses}")
            
            if self.preserve_ids:
                v1_cursor.execute(f"SELECT COUNT(DISTINCT user_id) as unique_users FROM {self.config.V1_ADDRESS_TABLE} WHERE user_id IS NOT NULL")
                unique_users = v1_cursor.fetchone()['unique_users']
                print(f"  Addresses linked to {unique_users} unique users")
        
        v1_cursor.close()
        
        existing_data = self.analyze_existing_data()
        has_existing = any(count > 0 for count in existing_data.values())
        
        if not self.select_migration_strategy(has_existing_data=has_existing):
            return False
        
        print(f"\n{Fore.CYAN}Migration settings:")
        if hasattr(self, 'migrate_users') and self.migrate_users:
            actual_migration_count = self.stats['users']['total_records'] - self.stats['users'].get('skipped_status_zero_pre', status_zero_count)
            print(f"  - Users table: {actual_migration_count} records (skipping {status_zero_count} with status=0)")
        if self.migrate_addresses:
            print(f"  - Address table: {self.stats['addresses']['total_records']} records")
        print(f"  - Migration mode: {self.migration_mode.upper()}")
        print(f"  - ID handling: {'Preserve original' if self.preserve_ids else 'Auto-increment'}")
        if hasattr(self, 'migrate_users') and self.migrate_users:
            print(f"  - Mobile format: Convert to +94")
            print(f"  - Gender format: M→Male, F→Female")
            if self.has_role_user_table:
                print(f"  - Role assignment: role_id=10 for all migrated users")
        
        response = input("\nContinue with migration? (yes/no): ").lower()
        return response == 'yes'

    def transform_user_record(self, record):
        """Transform a V1 user record to V2 format, including address JSON conversion."""
        try:
            # Combine names
            firstname = (record.get('firstname') or '').strip()
            lastname = (record.get('lastname') or '').strip()
            name = f"{firstname} {lastname}".strip() or f"User_{record['id']}"

            # Convert email verified flag to timestamp
            email_verified_at = datetime.strptime(self.config.DEFAULT_VERIFIED_TIMESTAMP, '%Y-%m-%d %H:%M:%S') if record.get('ev') == 1 else None

            # Convert mobile number
            mobile = self.convert_mobile_number(record.get('mobile'))

            # Convert gender
            gender = self.convert_gender(record.get('gender'))

            # Round balance
            balance = round(float(record.get('balance', 0)), 2)

            # --- Address Transformation ---
            address_str = None
            address_json = record.get('address')
            if address_json:
                try:
                    address_data = json.loads(address_json)
                    part_order = ['address', 'city', 'zip', 'state', 'country']
                    address_parts = [str(address_data.get(key)).strip() for key in part_order if address_data.get(key) is not None and str(address_data.get(key)).strip()]
                    if address_parts:
                        address_str = ','.join(address_parts)
                except (json.JSONDecodeError, TypeError):
                    self.logger.warning(f"Could not parse address JSON for user record {record['id']}. Using raw value. Value: {address_json}")
                    address_str = address_json if isinstance(address_json, str) else None

            result = {
                'operator_id': None,
                'name': name,
                'email': record.get('email'),
                'email_verified_at': email_verified_at,
                'password': record.get('password'),
                'two_factor_secret': None,
                'two_factor_recovery_codes': None,
                'two_factor_confirmed_at': None,
                'mobile': mobile,
                'gender': gender,
                'city_id': record.get('city_id'),
                'address': address_str,  # Use the transformed address string
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
                'otp_verified': 0,
                'v1_id': record['id']
            }
            
            if self.preserve_ids:
                result['id'] = record['id']
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error transforming user record {record['id']}: {e}")
            raise
    
    def transform_address_record(self, record):
        """Transform a V1 address record to V2 format"""
        try:
            result = dict(record)
            result['v1_id'] = record.get('id')
            
            if not self.preserve_ids and 'user_id' in record and record['user_id'] in self.id_mapping['users']:
                result['user_id'] = self.id_mapping['users'][record['user_id']]
                self.logger.debug(f"Updated address user_id: {record['user_id']} -> {result['user_id']}")
            
            if not self.preserve_ids and 'id' in result:
                del result['id']
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error transforming address record {record.get('id', 'unknown')}: {e}")
            raise
    
    def verify_role_user_table_structure(self):
        """Verify role_user table structure"""
        try:
            v2_cursor = self.v2_conn.cursor(dictionary=True)
            v2_cursor.execute(f"""
                SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT
                FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'role_user' ORDER BY ORDINAL_POSITION
            """, (self.config.V2_DATABASE,))
            
            columns = v2_cursor.fetchall()
            
            if columns:
                print(f"{Fore.CYAN}role_user table structure:")
                for col in columns:
                    print(f"  - {col['COLUMN_NAME']}: {col['DATA_TYPE']} (Nullable: {col['IS_NULLABLE']}, Default: {col['COLUMN_DEFAULT']})")
                
                column_names = [col['COLUMN_NAME'] for col in columns]
                required_cols = ['user_id', 'role_id']
                missing_cols = [col for col in required_cols if col not in column_names]
                
                if missing_cols:
                    print(f"{Fore.RED}⚠ Missing required columns in role_user: {missing_cols}")
                    return False
                else:
                    print(f"{Fore.GREEN}✓ role_user table has required columns")
                    return True
            else:
                print(f"{Fore.RED}⚠ Could not retrieve role_user table structure")
                return False
                
        except Exception as e:
            self.logger.error(f"Error verifying role_user table structure: {e}")
            print(f"{Fore.RED}⚠ Error checking role_user table: {e}")
            return False
        finally:
            v2_cursor.close()

    def insert_user_role(self, cursor, user_id, v1_id):
        """Insert user role assignment using provided cursor"""
        if not self.has_role_user_table:
            return
        
        try:
            insert_role_query = "INSERT IGNORE INTO role_user (user_id, role_id, created_at, updated_at) VALUES (%s, 10, NOW(), NOW())"
            
            self.logger.debug(f"Attempting to insert role for user_id={user_id}, role_id=10")
            cursor.execute(insert_role_query, (user_id,))
            rows_affected = cursor.rowcount
            
            if rows_affected > 0:
                self.stats['users']['role_assignments_success'] += 1
                self.logger.info(f"✓ Role assigned: user_id={user_id} (V1_ID={v1_id}) -> role_id=10")
            else:
                self.logger.debug(f"Role already exists for user_id {user_id} (V1 ID: {v1_id})")
            
        except Exception as e:
            self.stats['users']['role_assignments_failed'] += 1
            self.logger.error(f"✗ Failed to assign role for user_id {user_id} (V1 ID: {v1_id}): {e}")
    
    def build_migration_query(self, table_type='users'):
        """Build the appropriate migration query based on mode and table type"""
        if table_type == 'users':
            return self.build_users_migration_query()
        else:
            return self.build_address_migration_query()
    
    def build_users_migration_query(self):
        """Build users table migration query"""
        columns_list = [
            'operator_id', 'name', 'email', 'email_verified_at', 'password',
            'two_factor_secret', 'two_factor_recovery_codes', 'two_factor_confirmed_at',
            'mobile', 'gender', 'city_id', 'address', 'privacy_policy', 'terms_of_service',
            'postal_code', 'balance', 'remember_token', 'current_team_id', 
            'profile_photo_path', 'keycard', 'otp', 'otp_generated_at', 'public', 
            'status', 'created_by', 'updated_by', 'created_at', 'updated_at', 'otp_verified'
        ]
        if self.preserve_ids:
            columns_list.insert(0, 'id')
            
        base_columns = ", ".join(columns_list)
        value_placeholders = ", ".join([f"%({col})s" for col in columns_list])
        
        if self.migration_mode == 'skip':
            return f"INSERT IGNORE INTO {self.config.V2_TABLE} ({base_columns}) VALUES ({value_placeholders})"
        elif self.migration_mode == 'upsert':
            update_clause = ", ".join([f"{col} = VALUES({col})" for col in columns_list if col != 'id'])
            return f"INSERT INTO {self.config.V2_TABLE} ({base_columns}) VALUES ({value_placeholders}) ON DUPLICATE KEY UPDATE {update_clause}"
        else:
            return f"INSERT INTO {self.config.V2_TABLE} ({base_columns}) VALUES ({value_placeholders})"
    
    def build_address_migration_query(self):
        """Build dynamic address table migration query"""
        v1_cursor = self.v1_conn.cursor()
        v1_cursor.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s", (self.config.V1_DATABASE, self.config.V1_ADDRESS_TABLE))
        columns = [row[0] for row in v1_cursor.fetchall()]
        v1_cursor.close()
        
        if not self.preserve_ids and 'id' in columns:
            columns.remove('id')
        
        base_columns = ', '.join(columns)
        value_placeholders = ', '.join([f'%({col})s' for col in columns])
        
        if self.migration_mode == 'skip':
            return f"INSERT IGNORE INTO {self.config.V2_ADDRESS_TABLE} ({base_columns}) VALUES ({value_placeholders})"
        elif self.migration_mode == 'upsert':
            update_cols = [f"{col} = VALUES({col})" for col in columns if col not in ['id', 'user_id']]
            update_clause = ', '.join(update_cols)
            return f"INSERT INTO {self.config.V2_ADDRESS_TABLE} ({base_columns}) VALUES ({value_placeholders}) ON DUPLICATE KEY UPDATE {update_clause}"
        else:
            return f"INSERT INTO {self.config.V2_ADDRESS_TABLE} ({base_columns}) VALUES ({value_placeholders})"
    
    def migrate_batch(self, records, table_type='users'):
        """Migrate a batch of records"""
        v2_cursor = self.v2_conn.cursor()
        insert_query = self.build_migration_query(table_type)
        success_count = 0
        
        try:
            for record in records:
                try:
                    if table_type == 'users' and record.get('status') == 0:
                        self.stats['users']['skipped_status_zero'] += 1
                        self.logger.debug(f"Skipped user record {record['id']} with status=0")
                        continue
                    
                    transformed = self.transform_user_record(record) if table_type == 'users' else self.transform_address_record(record)
                    
                    v2_cursor.execute(insert_query, transformed)
                    rows_affected = v2_cursor.rowcount
                    
                    if self.migration_mode == 'skip' and rows_affected == 0:
                        self.stats[table_type]['skipped_records'] += 1
                    elif self.migration_mode == 'upsert' and rows_affected == 2:
                        self.stats[table_type]['updated_records'] += 1
                    else:
                        self.stats[table_type]['migrated_records'] += 1
                        new_id = v2_cursor.lastrowid if not self.preserve_ids else record['id']
                        if not self.preserve_ids:
                            self.id_mapping[table_type][record['id']] = new_id
                        if table_type == 'users':
                            self.insert_user_role(v2_cursor, new_id, record['id'])
                    
                    success_count += 1
                    
                except mysql.connector.IntegrityError as e:
                    error_msg = str(e)
                    if "Duplicate entry" in error_msg:
                        if "PRIMARY" in error_msg:
                            self.stats[table_type]['duplicate_key_errors'] += 1
                        elif "email_unique" in error_msg:
                            self.stats[table_type]['duplicate_email_errors'] += 1
                        elif "mobile_unique" in error_msg:
                            self.stats[table_type]['duplicate_mobile_errors'] += 1
                        if self.migration_mode == 'skip':
                            self.stats[table_type]['skipped_records'] += 1
                    self.failed_records[table_type].append({'record': record, 'error': str(e), 'error_type': 'IntegrityError'})
                    self.stats[table_type]['failed_records'] += 1
                    
                except Exception as e:
                    self.failed_records[table_type].append({'record': record, 'error': str(e), 'error_type': type(e).__name__})
                    self.stats[table_type]['failed_records'] += 1
            
            self.v2_conn.commit()
            
        except Exception as e:
            self.v2_conn.rollback()
            self.logger.error(f"Batch failed, rolled back: {e}")
            raise
        finally:
            v2_cursor.close()
        
        return success_count
    
    def migrate_table(self, table_type='users'):
        """Migrate a specific table"""
        source_table = self.config.V1_TABLE if table_type == 'users' else self.config.V1_ADDRESS_TABLE
        table_desc = "users" if table_type == 'users' else "addresses"
        print(f"\n{Fore.CYAN}Migrating {table_desc}...")
        
        v1_cursor = self.v1_conn.cursor(dictionary=True)
        base_query = f"SELECT * FROM {source_table} ORDER BY id"
        
        progress_bar = tqdm(total=self.stats[table_type]['total_records'], desc=f"Migrating {table_desc}", unit="records")
        
        offset = 0
        while True:
            v1_cursor.execute(f"{base_query} LIMIT {self.config.BATCH_SIZE} OFFSET {offset}")
            records = v1_cursor.fetchall()
            if not records:
                break
            
            self.migrate_batch(records, table_type)
            progress_bar.update(len(records))
            offset += self.config.BATCH_SIZE
        
        progress_bar.close()
        v1_cursor.close()
    
    def migrate(self):
        """Main migration process"""
        print(f"\n{Fore.CYAN}Starting migration... Mode: {self.migration_mode.upper()}")
        if hasattr(self, 'migrate_users') and self.migrate_users:
            self.migrate_table('users')
        if self.migrate_addresses:
            self.migrate_table('addresses')
        self.save_migration_report()
    
    def save_migration_report(self):
        """Save detailed migration report"""
        report = {
            'summary': self.stats,
            'failed_records': self.failed_records,
            'duplicate_emails': dict(self.duplicate_emails),
            'duplicate_mobiles': dict(self.duplicate_mobiles),
            'migration_mode': self.migration_mode,
            'preserve_ids': self.preserve_ids,
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        if not self.preserve_ids and any(self.id_mapping.values()):
            id_mapping_file = f"logs/id_mapping_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(id_mapping_file, 'w') as f:
                json.dump(self.id_mapping, f, indent=2)
            print(f"\n{Fore.CYAN}ID mapping saved to: {id_mapping_file}")
            report['id_mapping_file'] = id_mapping_file
        
        report_file = self.config.FAILED_RECORDS_FILE
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        if any(self.failed_records.values()):
            print(f"\n{Fore.YELLOW}Migration report saved to: {report_file}")
    
    def verify_role_assignments(self):
        """Verify role assignments in role_user table"""
        if not self.has_role_user_table:
            return
        
        try:
            v2_cursor = self.v2_conn.cursor(dictionary=True)
            v2_cursor.execute("SELECT COUNT(*) as count FROM role_user WHERE role_id = 10")
            role_10_count = v2_cursor.fetchone()['count']
            v2_cursor.execute(f"SELECT COUNT(*) as count FROM {self.config.V2_TABLE}")
            users_count = v2_cursor.fetchone()['count']
            
            print(f"\n{Fore.CYAN}Role Assignment Verification:")
            print(f"  Total users in users table: {users_count}")
            print(f"  Total role assignments (role_id=10): {role_10_count}")
            
            if role_10_count != self.stats['users']['migrated_records']:
                print(f"  {Fore.YELLOW}⚠ Mismatch between migrated users and role assignments!")
            else:
                print(f"  {Fore.GREEN}✓ All migrated users have role assignments!")
            
            v2_cursor.close()
            
        except Exception as e:
            self.logger.error(f"Error verifying role assignments: {e}")

    def post_migration_validation(self):
        """Validate migration results"""
        print(f"\n{Fore.CYAN}Running post-migration validation...")
        v2_cursor = self.v2_conn.cursor(dictionary=True)
        
        if hasattr(self, 'migrate_users') and self.migrate_users:
            v2_cursor.execute(f"SELECT COUNT(*) as count FROM {self.config.V2_TABLE}")
            v2_count = v2_cursor.fetchone()['count']
            
            print(f"\n{Fore.CYAN}Users Table Migration Summary:")
            print(f"  Total V1 records: {self.stats['users']['total_records']}")
            print(f"  Skipped (status=0): {Fore.YELLOW}{self.stats['users']['skipped_status_zero']}")
            print(f"  Successfully migrated: {Fore.GREEN}{self.stats['users']['migrated_records']}")
            if self.stats['users']['updated_records'] > 0: print(f"  Updated existing: {Fore.BLUE}{self.stats['users']['updated_records']}")
            if self.stats['users']['skipped_records'] > 0: print(f"  Skipped existing: {Fore.YELLOW}{self.stats['users']['skipped_records']}")
            print(f"  Failed records: {Fore.RED}{self.stats['users']['failed_records']}")
            print(f"  V2 table total count: {v2_count}")
            
            if self.has_role_user_table:
                print(f"\n{Fore.CYAN}Role Assignment Statistics:")
                print(f"  Successful assignments: {Fore.GREEN}{self.stats['users']['role_assignments_success']}")
                print(f"  Failed assignments: {Fore.RED}{self.stats['users']['role_assignments_failed']}")
                self.verify_role_assignments()
        
        if self.migrate_addresses:
            v2_cursor.execute(f"SELECT COUNT(*) as count FROM {self.config.V2_ADDRESS_TABLE}")
            v2_address_count = v2_cursor.fetchone()['count']
            
            print(f"\n{Fore.CYAN}Address Table Migration Summary:")
            print(f"  Total V1 records: {self.stats['addresses']['total_records']}")
            print(f"  Successfully migrated: {Fore.GREEN}{self.stats['addresses']['migrated_records']}")
            if self.stats['addresses']['updated_records'] > 0: print(f"  Updated existing: {Fore.BLUE}{self.stats['addresses']['updated_records']}")
            if self.stats['addresses']['skipped_records'] > 0: print(f"  Skipped existing: {Fore.YELLOW}{self.stats['addresses']['skipped_records']}")
            print(f"  Failed records: {Fore.RED}{self.stats['addresses']['failed_records']}")
            print(f"  V2 table total count: {v2_address_count}")
        
        v2_cursor.close()
    
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
            if not self.connect_databases(): return
            if not self.pre_migration_checks():
                print(f"{Fore.YELLOW}Migration cancelled by user")
                return
            self.migrate()
            self.post_migration_validation()
        except Exception as e:
            self.logger.error(f"Migration failed: {e}", exc_info=True)
            print(f"{Fore.RED}✗ Migration failed: {e}")
        finally:
            self.close_connections()
