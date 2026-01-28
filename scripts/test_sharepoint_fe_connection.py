#!/usr/bin/env python3
"""
Test script for SharePoint-FE connection workflow.
Tests:
1. List connections and verify sharepoint-fe exists
2. Create a test Lakeflow job using sharepoint-fe
3. Check job was created and stored
"""

import requests
import json
import time

BASE_URL = "http://localhost:8001"

def test_list_connections():
    """Test listing SharePoint connections from Unity Catalog"""
    print("=" * 60)
    print("TEST 1: List SharePoint Connections")
    print("=" * 60)
    
    response = requests.get(f"{BASE_URL}/sharepoint/connections")
    
    if response.status_code != 200:
        print(f"❌ Failed with status {response.status_code}")
        print(response.text)
        return None
    
    connections = response.json()
    print(f"✅ Found {len(connections)} SharePoint connections")
    
    # Find sharepoint-fe
    sharepoint_fe = None
    for conn in connections:
        if conn['id'] == 'sharepoint-fe':
            sharepoint_fe = conn
            break
    
    if sharepoint_fe:
        print(f"✅ Found 'sharepoint-fe' connection!")
        print(f"   Name: {sharepoint_fe['name']}")
        print(f"   ID: {sharepoint_fe['id']}")
        print(f"   Site ID: {sharepoint_fe.get('site_id', 'N/A')}")
    else:
        print(f"❌ 'sharepoint-fe' connection not found")
        print("Available connections:")
        for conn in connections:
            print(f"   - {conn['id']}")
    
    print()
    return sharepoint_fe


def test_create_lakeflow_job(connection):
    """Test creating a Lakeflow job with sharepoint-fe"""
    print("=" * 60)
    print("TEST 2: Create Lakeflow Job with sharepoint-fe")
    print("=" * 60)
    
    if not connection:
        print("❌ No connection provided")
        return None
    
    # Create test job
    job_data = {
        "connection_id": f"test-{int(time.time())}",  # Unique ID
        "connection_name": connection['name'],
        "source_schema": "test-site-id-123",  # Test SharePoint Site ID
        "destination_catalog": "main",
        "destination_schema": "sharepoint_fe_test"
    }
    
    print(f"Creating job with:")
    print(f"   Connection: {job_data['connection_name']}")
    print(f"   Site ID: {job_data['source_schema']}")
    print(f"   Destination: {job_data['destination_catalog']}.{job_data['destination_schema']}")
    print()
    
    response = requests.post(
        f"{BASE_URL}/api/lakeflow/jobs",
        json=job_data,
        timeout=60
    )
    
    if response.status_code == 200:
        result = response.json()
        print(f"✅ Job created successfully!")
        print(f"   Connection ID: {result.get('connection_id')}")
        print(f"   Pipeline ID: {result.get('document_pipeline_id')}")
        print(f"   Document Table: {result.get('document_table')}")
        return result
    else:
        print(f"❌ Failed with status {response.status_code}")
        print(f"   Response: {response.text}")
        return None


def test_list_lakeflow_jobs():
    """Test listing Lakeflow jobs"""
    print("=" * 60)
    print("TEST 3: List Lakeflow Jobs")
    print("=" * 60)
    
    response = requests.get(f"{BASE_URL}/api/lakeflow/jobs")
    
    if response.status_code != 200:
        print(f"❌ Failed with status {response.status_code}")
        return
    
    jobs = response.json()
    print(f"✅ Found {len(jobs)} Lakeflow job(s)")
    
    for i, job in enumerate(jobs, 1):
        print(f"\n   Job {i}:")
        print(f"      Connection: {job.get('connection_name')}")
        print(f"      Site ID: {job.get('source_schema')}")
        print(f"      Destination: {job.get('destination_catalog')}.{job.get('destination_schema')}")
        print(f"      Pipeline ID: {job.get('document_pipeline_id', 'N/A')}")
    
    print()


def cleanup_test_jobs():
    """Clean up test jobs"""
    print("=" * 60)
    print("CLEANUP: Remove test jobs")
    print("=" * 60)
    
    response = requests.get(f"{BASE_URL}/api/lakeflow/jobs")
    if response.status_code != 200:
        print("❌ Failed to list jobs for cleanup")
        return
    
    jobs = response.json()
    deleted_count = 0
    
    for job in jobs:
        if job['connection_id'].startswith('test-'):
            print(f"Deleting test job: {job['connection_id']}")
            delete_response = requests.delete(
                f"{BASE_URL}/api/lakeflow/jobs/{job['connection_id']}"
            )
            if delete_response.status_code == 200:
                print(f"   ✅ Deleted")
                deleted_count += 1
            else:
                print(f"   ❌ Failed: {delete_response.text}")
    
    if deleted_count > 0:
        print(f"\n✅ Cleaned up {deleted_count} test job(s)")
    else:
        print("\nℹ️  No test jobs to clean up")
    
    print()


def main():
    print("\n" + "=" * 60)
    print("SharePoint-FE Connection Test Suite")
    print("=" * 60)
    print()
    
    try:
        # Test 1: List connections
        connection = test_list_connections()
        
        if not connection:
            print("\n❌ Cannot proceed without sharepoint-fe connection")
            return
        
        # Test 2: Create Lakeflow job
        print("\n⚠️  NOTE: This will attempt to create a real Databricks pipeline.")
        print("   The pipeline creation may fail if the SharePoint Site ID is invalid.")
        print("   That's okay - we're testing the connection selection workflow.\n")
        
        input("Press Enter to continue with job creation test...")
        
        job = test_create_lakeflow_job(connection)
        
        # Test 3: List jobs
        time.sleep(2)  # Brief pause
        test_list_lakeflow_jobs()
        
        # Cleanup
        if job:
            print("\n⚠️  NOTE: A test pipeline was created in Databricks.")
            print("   You should delete it manually if the test job creation succeeded.\n")
        
        cleanup_input = input("Clean up test jobs from database? (y/n): ")
        if cleanup_input.lower() == 'y':
            cleanup_test_jobs()
        
        print("=" * 60)
        print("✅ Test Suite Complete!")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
