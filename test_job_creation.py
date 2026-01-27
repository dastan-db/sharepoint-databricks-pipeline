#!/usr/bin/env python3
"""
Test Lakeflow job creation with sharepoint-fe connection.
"""

import requests
import json

BASE_URL = "http://localhost:8001"

print("=" * 60)
print("Testing Lakeflow Job Creation with sharepoint-fe")
print("=" * 60)
print()

# Job data matching your inputs
job_data = {
    "connection_id": "test-sharepoint-fe-001",  # Unique ID for this test
    "connection_name": "sharepoint-fe",
    "source_schema": "6d152e54-1e19-45d9-a362-af47be1b3ba9",  # SharePoint Site ID
    "destination_catalog": "main",
    "destination_schema": "sharepoint_db"
}

print("Request Data:")
print(json.dumps(job_data, indent=2))
print()

print("Sending POST request to /api/lakeflow/jobs...")
print()

try:
    response = requests.post(
        f"{BASE_URL}/api/lakeflow/jobs",
        json=job_data,
        timeout=120  # 2 minutes for pipeline creation
    )
    
    print(f"Response Status: {response.status_code}")
    print()
    
    if response.status_code == 200:
        result = response.json()
        print("✅ SUCCESS!")
        print()
        print("Response:")
        print(json.dumps(result, indent=2))
    else:
        print("❌ FAILED!")
        print()
        print("Response:")
        try:
            error = response.json()
            print(json.dumps(error, indent=2))
        except:
            print(response.text)
            
except requests.exceptions.Timeout:
    print("❌ Request timed out (>120 seconds)")
    print("This might mean the pipeline is taking too long to create")
    
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()

print()
print("=" * 60)

