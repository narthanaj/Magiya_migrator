import mysql.connector
from colorama import init, Fore
import logging

init(autoreset=True)

class MigrationValidator:
    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger('MigrationValidator')
    
    def validate(self):
        """Run comprehensive validation checks"""
        print(f"\n{Fore.CYAN}Running comprehensive validation...")
        
        try:
            v1_conn = mysql.connector.connect(**self.config.V1_CONFIG)
            v2_conn = mysql.connector.connect(**self.config.V2_CONFIG)
            
            v1_cursor = v1_conn.cursor(dictionary=True)
            v2_cursor = v2_conn.cursor(dictionary=True)
            
            # 1. Record count validation
            v1_cursor.execute(f"SELECT COUNT(*) as count FROM {self.config.V1_TABLE}")
            v1_count = v1_cursor.fetchone()['count']
            
            v2_cursor.execute(f"SELECT COUNT(*) as count FROM {self.config.V2_TABLE}")
            v2_count = v2_cursor.fetchone()['count']
            
            print(f"\n{Fore.CYAN}Record Count Validation:")
            print(f"  V1 records: {v1_count}")
            print(f"  V2 records: {v2_count}")
            print(f"  Difference: {v1_count - v2_count}")
            
            if v1_count == v2_count:
                print(f"  {Fore.GREEN}✓ Record counts match!")
            else:
                print(f"  {Fore.RED}✗ Record count mismatch!")
            
            # 2. Data integrity checks
            print(f"\n{Fore.CYAN}Data Integrity Checks:")
            
            # Check email verified conversion
            v1_cursor.execute(f"""
                SELECT COUNT(*) as count 
                FROM {self.config.V1_TABLE} 
                WHERE ev = 1
            """)
            v1_verified = v1_cursor.fetchone()['count']
            
            v2_cursor.execute(f"""
                SELECT COUNT(*) as count 
                FROM {self.config.V2_TABLE} 
                WHERE email_verified_at IS NOT NULL
            """)
            v2_verified = v2_cursor.fetchone()['count']
            
            print(f"  Email verified - V1: {v1_verified}, V2: {v2_verified}")
            if v1_verified == v2_verified:
                print(f"  {Fore.GREEN}✓ Email verification counts match!")
            else:
                print(f"  {Fore.RED}✗ Email verification mismatch!")
            
            # 3. Sample data comparison
            print(f"\n{Fore.CYAN}Sample Data Comparison:")
            
            # Get sample records from V1
            v1_cursor.execute(f"""
                SELECT id, firstname, lastname, email, mobile, balance, ev
                FROM {self.config.V1_TABLE}
                ORDER BY id
                LIMIT 10
            """)
            v1_samples = v1_cursor.fetchall()
            
            for v1_record in v1_samples:
                # Get corresponding V2 record
                v2_cursor.execute(f"""
                    SELECT name, email, mobile, balance, email_verified_at
                    FROM {self.config.V2_TABLE}
                    WHERE id = %s
                """, (v1_record['id'],))
                
                v2_record = v2_cursor.fetchone()
                
                if v2_record:
                    # Check name combination
                    expected_name = f"{(v1_record['firstname'] or '').strip()} {(v1_record['lastname'] or '').strip()}".strip()
                    if not expected_name:
                        expected_name = f"User_{v1_record['id']}"
                    
                    name_match = v2_record['name'] == expected_name
                    
                    print(f"\n  ID {v1_record['id']}:")
                    print(f"    Name: {'✓' if name_match else '✗'} "
                          f"'{v1_record['firstname']} {v1_record['lastname']}' -> '{v2_record['name']}'")
                    print(f"    Email: {'✓' if v1_record['email'] == v2_record['email'] else '✗'}")
                    print(f"    Balance: {v1_record['balance']} -> {v2_record['balance']}")
                else:
                    print(f"\n  {Fore.RED}✗ Record {v1_record['id']} not found in V2!")
            
            # 4. Check for data issues
            print(f"\n{Fore.CYAN}Data Quality Checks:")
            
            # Empty names in V2
            v2_cursor.execute(f"""
                SELECT COUNT(*) as count 
                FROM {self.config.V2_TABLE} 
                WHERE name IS NULL OR name = ''
            """)
            empty_names = v2_cursor.fetchone()['count']
            print(f"  Empty names in V2: {empty_names}")
            
            # Truncated mobiles
            v2_cursor.execute(f"""
                SELECT COUNT(*) as count 
                FROM {self.config.V2_TABLE} 
                WHERE LENGTH(mobile) = 13
            """)
            max_length_mobiles = v2_cursor.fetchone()['count']
            print(f"  Mobile numbers at max length (possibly truncated): {max_length_mobiles}")
            
            v1_cursor.close()
            v2_cursor.close()
            v1_conn.close()
            v2_conn.close()
            
            print(f"\n{Fore.GREEN}✓ Validation completed!")
            
        except Exception as e:
            self.logger.error(f"Validation failed: {e}")
            print(f"{Fore.RED}✗ Validation failed: {e}")