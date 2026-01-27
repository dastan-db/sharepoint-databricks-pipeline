#!/usr/bin/env python3
"""
Simple verification that sharepoint-fe connection is available.
"""

import requests
import json

BASE_URL = "http://localhost:8001"

print("=" * 60)
print("SharePoint-FE Connection Verification")
print("=" * 60)
print()

# Test connection listing
print("Fetching SharePoint connections from Unity Catalog...")
response = requests.get(f"{BASE_URL}/sharepoint/connections")

if response.status_code != 200:
    print(f"❌ Failed: {response.status_code}")
    print(response.text)
    exit(1)

connections = response.json()
print(f"✅ Found {len(connections)} SharePoint connection(s)")
print()

# Find sharepoint-fe
sharepoint_fe = None
for conn in connections:
    if conn['id'] == 'sharepoint-fe':
        sharepoint_fe = conn
        break

if sharepoint_fe:
    print("✅ SUCCESS: 'sharepoint-fe' connection is available!")
    print()
    print("Connection Details:")
    print(f"   ID: {sharepoint_fe['id']}")
    print(f"   Name: {sharepoint_fe['name']}")
    print(f"   Type: SHAREPOINT (Unity Catalog)")
    print(f"   Site ID: {sharepoint_fe.get('site_id') or '(not set)'}")
    print()
    print("📋 Next Steps:")
    print("   1. Open http://localhost:8001 in your browser")
    print("   2. You'll see 'sharepoint-fe' in the SharePoint Connections table")
    print("   3. Click the radio button to select it")
    print("   4. Fill in the SharePoint Site ID in the Lakeflow Job form")
    print("   5. Configure destination catalog/schema")
    print("   6. Click 'Create Job' to create a Lakeflow pipeline")
    print()
else:
    print("❌ 'sharepoint-fe' connection not found!")
    print()
    print("Available connections:")
    for i, conn in enumerate(connections, 1):
        print(f"   {i}. {conn['id']}")
    print()
    print("💡 You can use any of the above connections for testing.")

print("=" * 60)
print()
