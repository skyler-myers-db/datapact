#!/usr/bin/env python
"""Test script to validate datapact functionality."""

from databricks.sdk import WorkspaceClient
from datapact.client import DataPactClient
from datapact.config import DataPactConfig
import yaml
import sys

def main():
    print("=" * 60)
    print("DATAPACT VALIDATION TEST")
    print("=" * 60)
    
    # Load config
    print("\n1. Loading configuration...")
    with open('test_config.yml', 'r') as f:
        config = DataPactConfig(**yaml.safe_load(f))
    print(f"   ✅ Loaded {len(config.validations)} validations")
    
    # Create client
    print("\n2. Initializing DataPact client...")
    client = DataPactClient()
    print(f"   ✅ User: {client.user_name}")
    print(f"   ✅ Root: {client.root_path}")
    
    # Test SQL generation
    print("\n3. Testing SQL generation...")
    try:
        sql = client._generate_genie_room_sql(
            "`datapact`.`results`.`run_history`",
            "Test Job"
        )
        print("   ✅ Genie room SQL generated")
    except Exception as e:
        print(f"   ❌ Failed: {e}")
        return 1
    
    # Test upload
    print("\n4. Testing file upload...")
    try:
        asset_paths = client._upload_sql_scripts(
            config=config,
            results_table="`datapact`.`results`.`run_history`",
            job_name="Test Job"
        )
        print(f"   ✅ Uploaded {len(asset_paths)} files:")
        for key in asset_paths:
            print(f"      - {key}")
    except Exception as e:
        print(f"   ❌ Failed: {e}")
        return 1
    
    # Test warehouse
    print("\n5. Testing warehouse access...")
    try:
        warehouses = list(client.w.warehouses.list())
        print(f"   ✅ Found {len(warehouses)} warehouses")
        running = [w for w in warehouses if str(w.state) == "State.RUNNING"]
        if running:
            print(f"   ✅ Running warehouse: {running[0].name}")
        else:
            print("   ⚠️  No running warehouses")
    except Exception as e:
        print(f"   ❌ Failed: {e}")
        return 1
    
    print("\n" + "=" * 60)
    print("✅ ALL TESTS PASSED - DATAPACT IS READY!")
    print("=" * 60)
    
    print("\nYou can now run:")
    print("  datapact run --config demo/demo_config.yml --warehouse <your-warehouse>")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())