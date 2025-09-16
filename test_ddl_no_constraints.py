#!/usr/bin/env python3
"""
Test that the updated DeepSeek prompt generates DDL without NOT NULL constraints.
"""

import os
import sys

# Add project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from oracle_to_postgres.common.deepseek_client import DeepSeekClient
from oracle_to_postgres.common.config import Config


def test_prompt_generation():
    """Test that the prompt includes instructions to avoid NOT NULL constraints."""
    print("Testing DeepSeek Prompt Generation")
    print("=" * 50)
    
    # Load config
    config = Config.from_file('config.yaml')
    
    # Create DeepSeek client (we won't actually call the API)
    client = DeepSeekClient(
        api_key="test-key",  # Dummy key for testing
        base_url=config.deepseek.base_url,
        model=config.deepseek.model,
        timeout=config.deepseek.timeout,
        max_retries=config.deepseek.max_retries,
        max_samples=config.max_insert_samples
    )
    
    # Sample INSERT statements that might have NULL values
    sample_inserts = [
        "INSERT INTO CRZYMXB (NY, BAH, ZYH, CZLX, LYKS, MDKS) VALUES ('2019-01-12', NULL, '02452755', 'zy', '0003', '0011')",
        "INSERT INTO CRZYMXB (NY, BAH, ZYH, CZLX, LYKS, MDKS) VALUES ('2019-01-13', 'b37a635a6aa780814b83', '02452756', 'zy', '0003', '0011')",
        "INSERT INTO CRZYMXB (NY, BAH, ZYH, CZLX, LYKS, MDKS) VALUES ('2019-01-14', NULL, '02452757', 'zy', '0003', NULL)"
    ]
    
    # Build the prompt
    prompt = client._build_prompt("CRZYMXB", sample_inserts)
    
    print("Generated Prompt:")
    print("-" * 40)
    print(prompt)
    print("-" * 40)
    
    # Check that the prompt includes the correct instructions
    checks = [
        {
            'text': 'DO NOT add NOT NULL constraints',
            'description': 'Explicitly forbids NOT NULL constraints'
        },
        {
            'text': 'this is for data analysis',
            'description': 'Explains the use case'
        },
        {
            'text': 'allow all columns to be nullable',
            'description': 'Explicitly allows NULL values'
        },
        {
            'text': 'DO NOT add PRIMARY KEY constraints',
            'description': 'Forbids PRIMARY KEY constraints'
        },
        {
            'text': 'Keep the DDL simple and permissive',
            'description': 'Emphasizes simplicity'
        }
    ]
    
    print("\nPrompt Analysis:")
    print("-" * 40)
    
    all_passed = True
    for check in checks:
        if check['text'] in prompt:
            print(f"✅ PASS: {check['description']}")
        else:
            print(f"❌ FAIL: {check['description']}")
            all_passed = False
    
    # Check that old problematic instructions are removed
    problematic_texts = [
        'Use NOT NULL only when confident',
        'Add PRIMARY KEY on',
        'NOT NULL,'
    ]
    
    print("\nChecking for problematic instructions:")
    print("-" * 40)
    
    for text in problematic_texts:
        if text in prompt:
            print(f"❌ FOUND: '{text}' - should be removed")
            all_passed = False
        else:
            print(f"✅ CLEAN: '{text}' - not found")
    
    return all_passed


def show_expected_ddl_format():
    """Show what the expected DDL format should look like."""
    print("\n" + "=" * 50)
    print("Expected DDL Format")
    print("=" * 50)
    
    print("BEFORE (Problematic with constraints):")
    print("""CREATE TABLE "CRZYMXB" (
    "NY" DATE NOT NULL,
    "BAH" VARCHAR(50) NOT NULL,
    "ZYH" VARCHAR(20) NOT NULL,
    "CZLX" VARCHAR(10) NOT NULL,
    "LYKS" VARCHAR(20) NOT NULL,
    "MDKS" VARCHAR(20) NOT NULL,
    PRIMARY KEY ("NY", "BAH")
);""")
    
    print("\nAFTER (Correct without constraints):")
    print("""CREATE TABLE "CRZYMXB" (
    "NY" DATE,
    "BAH" VARCHAR(50),
    "ZYH" VARCHAR(20),
    "CZLX" VARCHAR(10),
    "LYKS" VARCHAR(20),
    "MDKS" VARCHAR(20)
);""")
    
    print("\nKey Differences:")
    print("✅ No NOT NULL constraints")
    print("✅ No PRIMARY KEY constraints")
    print("✅ All columns can accept NULL values")
    print("✅ Simple and permissive for data analysis")


def main():
    """Main test function."""
    print("DeepSeek DDL Generation - No Constraints Test")
    print("=" * 60)
    
    try:
        success = test_prompt_generation()
        show_expected_ddl_format()
        
        print("\n" + "=" * 60)
        if success:
            print("🎉 SUCCESS: Prompt updated correctly!")
            print("=" * 60)
            print("The DeepSeek prompt now:")
            print("1. ✅ Explicitly forbids NOT NULL constraints")
            print("2. ✅ Explicitly forbids PRIMARY KEY constraints")
            print("3. ✅ Explains this is for data analysis use case")
            print("4. ✅ Emphasizes permissive DDL generation")
            print("5. ✅ Allows all columns to be nullable")
            print()
            print("This should resolve the NOT NULL constraint violations!")
            print("You may need to regenerate the DDL files with:")
            print("  python analyze_sql.py -c config.yaml")
        else:
            print("❌ ISSUES FOUND: Some prompt updates may be missing")
            
    except Exception as e:
        print(f"❌ TEST FAILED: {e}")


if __name__ == "__main__":
    main()