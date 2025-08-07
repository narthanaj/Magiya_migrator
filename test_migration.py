import mysql.connector
from config import Config

def test_specific_records():
    """Test specific edge cases"""
    config = Config()
    
    # Test cases to check
    test_cases = [
        "Records with both firstname and lastname",
        "Records with only firstname",
        "Records with only lastname",
        "Records with neither firstname nor lastname",
        "Records with ev=1 (verified emails)",
        "Records with long mobile numbers",
        "Records with high balance values"
    ]
    
    v2_conn = mysql.connector.connect(**config.V2_CONFIG)
    v2_cursor = v2_conn.cursor(dictionary=True)
    
    print("Running specific tests...\n")
    
    # Test 1: Name combinations
    v2_cursor.execute(f"""
        SELECT id, name FROM {config.V2_TABLE} 
        WHERE name LIKE '% %' LIMIT 5
    """)
    print("Records with full names (space in name):")
    for record in v2_cursor.fetchall():
        print(f"  ID: {record['id']}, Name: '{record['name']}'")
    
    # Test 2: Email verification
    v2_cursor.execute(f"""
        SELECT COUNT(*) as count, MIN(email_verified_at) as min_time 
        FROM {config.V2_TABLE} 
        WHERE email_verified_at IS NOT NULL
    """)
    result = v2_cursor.fetchone()
    print(f"\nEmail verification: {result['count']} verified emails")
    print(f"  Timestamp used: {result['min_time']}")
    
    v2_cursor.close()
    v2_conn.close()

if __name__ == "__main__":
    test_specific_records()