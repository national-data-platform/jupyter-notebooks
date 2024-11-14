import pytest
import random
import string
from scidx.client import sciDXClient

# Constants
API_URL = "http://localhost:8000"
OWNER_ORG = "test_org"
USERNAME = "placeholder@placeholder.com"
PASSWORD = "placeholder"

def generate_unique_name():
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=6))

@pytest.fixture
def client():
    """
    Fixture to create and login the client for testing.
    """
    client = sciDXClient(API_URL)
    client.login(USERNAME, PASSWORD)
    return client

def verify_resource_exists(client, search_term, expected_name):
    """
    Verify that the resource exists in the search results.
    """
    search_results = client.search_resource(search_term=search_term)
    assert len(search_results) > 0, f"No resources found for search term: {search_term}"
    found = any(result["name"] == expected_name for result in search_results)
    assert found, f"Resource with name '{expected_name}' not found in search results."
    return search_results[0]  # Return the first match for further verification

def test_s3_resource_flow(client):
    """
    Test registering, retrieving, and updating an S3 resource.
    """
    resource_name = "s3_resource_" + generate_unique_name()
    
    # Step 1: Register the S3 resource with incorrect information
    incorrect_s3_data = {
        "resource_name": resource_name,
        "resource_title": "Incorrect S3 Resource Title",
        "owner_org": OWNER_ORG,
        "resource_s3": "s3://wrong-bucket/resource",
        "notes": "This is an incorrect S3 resource for testing.",
        "extras": {"wrong_key1": "wrong_value1", "wrong_key2": "wrong_value2"}
    }

    print("\n=== Registering S3 Resource with Incorrect Information ===")
    response = client.register_s3(**incorrect_s3_data)
    print(f"Registered Incorrect S3 Resource Response: {response}")
    resource_id = response.get("id")
    assert resource_id, "Failed to register S3 resource with incorrect information"

    # Retrieve and verify the resource
    print("\n=== Verifying Registered S3 Resource ===")
    resource = verify_resource_exists(client, search_term=resource_name, expected_name=resource_name)
    print(f"Retrieved Resource: {resource}")

    # Step 2: Update the resource with correct information
    updated_name = "updated_s3_resource_" + generate_unique_name()
    correct_s3_data = {
        "resource_name": updated_name,
        "resource_title": "Correct S3 Resource Title",
        "resource_s3": "s3://example-bucket/resource",
        "notes": "This is a correct S3 resource for testing.",
        "extras": {"key1": "value1", "key2": "value2"}
    }

    print("\n=== Updating S3 Resource with Correct Information ===")
    update_response = client.update_s3(resource_id=resource_id, **correct_s3_data)
    print(f"Updated S3 Resource Response: {update_response}")
    assert update_response.get("message") == "S3 resource updated successfully", "Failed to update S3 resource"

    # Step 3: Verify the updated resource
    print("\n=== Verifying Updated S3 Resource ===")
    updated_resource = verify_resource_exists(client, search_term=updated_name, expected_name=updated_name)
    print(f"Updated Resource: {updated_resource}")
    assert updated_resource["extras"]["key1"] == "value1", "Extras field not updated correctly"
    assert updated_resource["extras"]["key2"] == "value2", "Extras field not updated correctly"

if __name__ == "__main__":
    pytest.main([__file__])
