import mysql.connector
from colorama import init, Fore
import json

init(autoreset=True)

class DuplicateResolver:
    def __init__(self, config):
        self.config = config
        
    def analyze_duplicates(self):
        """Analyze and fix duplicate issues in V1"""
        print(f"\n{Fore.CYAN}Analyzing duplicates in source database...")
        
        v1_conn = mysql.connector.connect(**self.config.V1_CONFIG)
        v1_cursor = v1_conn.cursor(dictionary=True)
        
        # Analyze duplicate emails
        print(f"\n{Fore.YELLOW}Duplicate Emails:")
        v1_cursor.execute(f"""
            SELECT email, GROUP_CONCAT(id) as ids, COUNT(*) as count 
            FROM {self.config.V1_TABLE} 
            WHERE email IS NOT NULL AND email != ''
            GROUP BY email 
            HAVING count > 1
            ORDER BY count DESC
            LIMIT 20
        """)
        
        dup_emails = v1_cursor.fetchall()
        for dup in dup_emails:
            print(f"  {dup['email']}: {dup['count']} records (IDs: {dup['ids']})")
        
        # Analyze duplicate mobiles
        print(f"\n{Fore.YELLOW}Duplicate Mobile Numbers:")
        v1_cursor.execute(f"""
            SELECT mobile, GROUP_CONCAT(id) as ids, COUNT(*) as count 
            FROM {self.config.V1_TABLE} 
            WHERE mobile IS NOT NULL AND mobile != ''
            GROUP BY mobile 
            HAVING count > 1
            ORDER BY count DESC
            LIMIT 20
        """)
        
        dup_mobiles = v1_cursor.fetchall()
        for dup in dup_mobiles:
            print(f"  {dup['mobile']}: {dup['count']} records (IDs: {dup['ids']})")
        
        v1_cursor.close()
        v1_conn.close()
        
        # Offer resolution options
        print(f"\n{Fore.CYAN}Resolution Options:")
        print("1. Generate SQL to fix duplicates (keep first occurrence)")
        print("2. Generate SQL to fix duplicates (keep last occurrence)")
        print("3. Export duplicate records for manual review")
        print("4. Return to main menu")
        
        choice = input("\nEnter your choice (1-4): ")
        
        if choice in ['1', '2']:
            self.generate_dedup_sql(keep_first=(choice == '1'))
        elif choice == '3':
            self.export_duplicates()
    
    def generate_dedup_sql(self, keep_first=True):
        """Generate SQL to fix duplicates"""
        order = "ASC" if keep_first else "DESC"
        
        sql_file = f"fix_duplicates_{'first' if keep_first else 'last'}.sql"
        
        with open(sql_file, 'w') as f:
            f.write("-- SQL to fix duplicate emails and mobiles\n")
            f.write("-- Backup your data before running this!\n\n")
            
            f.write("-- Fix duplicate emails\n")
            f.write(f"""
UPDATE {self.config.V1_TABLE} t1
JOIN (
    SELECT email, MIN(id) as keep_id
    FROM {self.config.V1_TABLE}
    WHERE email IS NOT NULL AND email != ''
    GROUP BY email
    HAVING COUNT(*) > 1
) t2 ON t1.email = t2.email AND t1.id != t2.keep_id
SET t1.email = CONCAT(t1.email, '_DUP_', t1.id);

-- Fix duplicate mobiles
UPDATE {self.config.V1_TABLE} t1
JOIN (
    SELECT mobile, MIN(id) as keep_id
    FROM {self.config.V1_TABLE}
    WHERE mobile IS NOT NULL AND mobile != ''
    GROUP BY mobile
    HAVING COUNT(*) > 1
) t2 ON t1.mobile = t2.mobile AND t1.id != t2.keep_id
SET t1.mobile = CONCAT(t1.mobile, '_DUP_', t1.id);
""")
        
        print(f"\n{Fore.GREEN}✓ SQL script generated: {sql_file}")
        print(f"  Review and run this script on your V1 database to fix duplicates")
    
    def export_duplicates(self):
        """Export duplicate records for review"""
        # Implementation for exporting duplicates to CSV
        pass