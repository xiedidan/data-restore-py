#!/usr/bin/env python3
"""
Check the actual table structure in PostgreSQL to understand the column name issue.
"""

import os
import sys
import psycopg2

# Add project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from oracle_to_postgres.common.config import Config


def check_table_structure():
    """Check the actual table structure in PostgreSQL."""
    print("Checking PostgreSQL Table Structure")
    print("=" * 50)
    
    # Load config
    config = Config.from_file('config.yaml')
    
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=config.postgresql.host,
            port=config.postgresql.port,
            database=config.postgresql.database,
            user=config.postgresql.username,
            password=config.postgresql.password
        )
        
        cursor = conn.cursor()
        
        # Check if CRZYMXB table exists and get its structure
        print("1. Checking if CRZYMXB table exists...")
        cursor.execute("""
            SELECT table_name, table_schema 
            FROM information_schema.tables 
            WHERE table_name ILIKE 'CRZYMXB'
        """)
        
        tables = cursor.fetchall()
        if tables:
            for table_name, schema in tables:
                print(f"✓ Found table: {schema}.{table_name}")
        else:
            print("✗ CRZYMXB table not found")
            
            # Check for similar table names
            print("\nLooking for similar table names...")
            cursor.execute("""
                SELECT table_name, table_schema 
                FROM information_schema.tables 
                WHERE table_name ILIKE '%CZRY%' OR table_name ILIKE '%MXB%'
                ORDER BY table_name
            """)
            
            similar_tables = cursor.fetchall()
            if similar_tables:
                print("Similar tables found:")
                for table_name, schema in similar_tables:
                    print(f"  - {schema}.{table_name}")
            else:
                print("No similar tables found")
        
        # Check column structure for CRZYMXB (case-insensitive)
        print("\n2. Checking column structure...")
        cursor.execute("""
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns 
            WHERE table_name ILIKE 'CRZYMXB'
            ORDER BY ordinal_position
        """)
        
        columns = cursor.fetchall()
        if columns:
            print("Columns in CRZYMXB table:")
            print("-" * 40)
            for col_name, data_type, nullable, default in columns:
                print(f"  {col_name} ({data_type}) {'NULL' if nullable == 'YES' else 'NOT NULL'}")
                
            # Check specifically for 'NY' column variations
            print("\n3. Checking for 'NY' column variations...")
            ny_columns = [col for col in columns if 'ny' in col[0].lower()]
            if ny_columns:
                print("Found NY-related columns:")
                for col_name, data_type, nullable, default in ny_columns:
                    print(f"  ✓ {col_name} ({data_type})")
            else:
                print("✗ No NY-related columns found")
                
        else:
            print("✗ No columns found for CRZYMXB table")
        
        # Test the actual INSERT statement that's failing
        print("\n4. Testing problematic INSERT statement...")
        test_insert = '''INSERT INTO "public"."CRZYMXB" (NY, BAH, ZYH, CZLX, LYKS, MDKS) VALUES ('test', 'test', 'test', 'test', 'test', 'test')'''
        
        try:
            cursor.execute("BEGIN")
            cursor.execute(test_insert)
            cursor.execute("ROLLBACK")  # Don't actually insert
            print("✓ INSERT statement syntax is correct")
        except Exception as e:
            print(f"✗ INSERT statement failed: {str(e)}")
            
            # Try with lowercase column names
            print("\n5. Testing with lowercase column names...")
            test_insert_lower = '''INSERT INTO "public"."CRZYMXB" (ny, bah, zyh, czlx, lyks, mdks) VALUES ('test', 'test', 'test', 'test', 'test', 'test')'''
            
            try:
                cursor.execute("BEGIN")
                cursor.execute(test_insert_lower)
                cursor.execute("ROLLBACK")
                print("✓ INSERT with lowercase column names works!")
                print("SOLUTION: Column names need to be lowercase")
            except Exception as e2:
                print(f"✗ INSERT with lowercase also failed: {str(e2)}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Database connection failed: {str(e)}")


def main():
    """Main function."""
    check_table_structure()
    
    print("\n" + "=" * 50)
    print("DIAGNOSIS")
    print("=" * 50)
    print("The error 'column \"ny\" of relation \"CRZYMXB\" does not exist' suggests:")
    print("1. The table CRZYMXB exists")
    print("2. But the column names might be in different case")
    print("3. PostgreSQL is case-sensitive with quoted identifiers")
    print()
    print("Possible solutions:")
    print("1. Convert column names to lowercase in INSERT statements")
    print("2. Match the exact case used when the table was created")
    print("3. Use unquoted identifiers (PostgreSQL converts to lowercase)")


if __name__ == "__main__":
    main()