import pytest
import random
import string
from scidx.client import sciDXClient, StreamProcessing, CSVProcessing, TXTProcessing, JSONProcessing, NetCDFProcessing

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

def test_stream_url(client):
    """
    Test registering, retrieving, and updating a Stream URL resource.
    """
    resource_name = "stream_resource_" + generate_unique_name()
    stream_processing = StreamProcessing(refresh_rate="5 seconds", data_key="results")
    
    # Register the resource
    print("\n=== Registering Stream Resource ===")
    response = client.register_url(
        resource_name=resource_name,
        resource_title="Stream Resource Example",
        owner_org=OWNER_ORG,
        resource_url="http://example.com/stream",
        file_type="stream",
        processing=stream_processing,
        notes="This is a stream resource for testing.",
        extras={"key1": "value1", "key2": "value2"}
    )
    print(f"Registered Stream Resource Response: {response}")
    resource_id = response.get("id")
    assert resource_id, "Failed to register stream resource"

    # Retrieve and verify the resource
    print("\n=== Verifying Registered Stream Resource ===")
    resource = verify_resource_exists(client, search_term=resource_name, expected_name=resource_name)
    print(f"Retrieved Resource: {resource}")

    # Update the resource
    updated_name = "updated_stream_resource_" + generate_unique_name()
    updated_processing = StreamProcessing(refresh_rate="10 seconds", data_key="new_results")
    print("\n=== Updating Stream Resource ===")
    update_response = client.update_url(
        resource_id=resource_id,
        resource_name=updated_name,
        resource_title="Updated Stream Resource",
        processing=updated_processing,
        notes="This is an updated stream resource for testing."
    )
    print(f"Updated Stream Resource Response: {update_response}")
    assert update_response.get("message") == "Resource updated successfully", "Failed to update stream resource"

    # Verify the updated resource
    print("\n=== Verifying Updated Stream Resource ===")
    updated_resource = verify_resource_exists(client, search_term=updated_name, expected_name=updated_name)
    print(f"Updated Resource: {updated_resource}")
    assert updated_resource["extras"]["processing"]["refresh_rate"] == "10 seconds", "Processing field not updated correctly"
    
    # Ensure the resource is deleted at the end of the test
    print("\n=== Deleting Stream Resource ===")
    delete_response = client.delete_resource(resource_id)
    print(f"Deleted resource: {delete_response}")
    assert delete_response.get("message") == f"{resource_id} deleted successfully", "Failed to delete stream resource"


def test_csv_url(client):
    """
    Test registering, retrieving, and updating a CSV URL resource.
    """
    resource_name = "csv_resource_" + generate_unique_name()
    csv_processing = CSVProcessing(delimiter=",", header_line=1, start_line=2)
    
    # Register the resource
    print("\n=== Registering CSV Resource ===")
    response = client.register_url(
        resource_name=resource_name,
        resource_title="CSV Resource Example",
        owner_org=OWNER_ORG,
        resource_url="http://example.com/csv",
        file_type="CSV",
        processing=csv_processing,
        notes="This is a CSV resource for testing.",
        extras={"key1": "value1", "key2": "value2"}
    )
    print(f"Registered CSV Resource Response: {response}")
    resource_id = response.get("id")
    assert resource_id, "Failed to register CSV resource"

    # Retrieve and verify the resource
    print("\n=== Verifying Registered CSV Resource ===")
    resource = verify_resource_exists(client, search_term=resource_name, expected_name=resource_name)
    print(f"Retrieved Resource: {resource}")

    # Update the resource
    updated_name = "updated_csv_resource_" + generate_unique_name()
    updated_processing = CSVProcessing(delimiter=";", header_line=2, start_line=3)
    print("\n=== Updating CSV Resource ===")
    update_response = client.update_url(
        resource_id=resource_id,
        resource_name=updated_name,
        resource_title="Updated CSV Resource",
        processing=updated_processing,
        notes="This is an updated CSV resource for testing."
    )
    print(f"Updated CSV Resource Response: {update_response}")
    assert update_response.get("message") == "Resource updated successfully", "Failed to update CSV resource"

    # Verify the updated resource
    print("\n=== Verifying Updated CSV Resource ===")
    updated_resource = verify_resource_exists(client, search_term=updated_name, expected_name=updated_name)
    print(f"Updated Resource: {updated_resource}")
    assert updated_resource["extras"]["processing"]["delimiter"] == ";", "Processing field not updated correctly"
    
    # Ensure the resource is deleted at the end of the test
    print("\n=== Deleting CSV Resource ===")
    delete_response = client.delete_resource(resource_id)
    print(f"Deleted resource: {delete_response}")
    assert delete_response.get("message") == f"{resource_id} deleted successfully", "Failed to delete stream resource"

def test_txt_url(client):
    """
    Test registering, retrieving, and updating a TXT URL resource.
    """
    resource_name = "txt_resource_" + generate_unique_name()
    txt_processing = TXTProcessing(delimiter="\t", header_line=1, start_line=2)
    
    # Register the resource
    print("\n=== Registering TXT Resource ===")
    response = client.register_url(
        resource_name=resource_name,
        resource_title="TXT Resource Example",
        owner_org=OWNER_ORG,
        resource_url="http://example.com/txt",
        file_type="TXT",
        processing=txt_processing,
        notes="This is a TXT resource for testing.",
        extras={"key1": "value1", "key2": "value2"}
    )
    print(f"Registered TXT Resource Response: {response}")
    resource_id = response.get("id")
    assert resource_id, "Failed to register TXT resource"

    # Retrieve and verify the resource
    print("\n=== Verifying Registered TXT Resource ===")
    resource = verify_resource_exists(client, search_term=resource_name, expected_name=resource_name)
    print(f"Retrieved Resource: {resource}")

    # Update the resource
    updated_name = "updated_txt_resource_" + generate_unique_name()
    updated_processing = TXTProcessing(delimiter=",", header_line=3, start_line=4)
    print("\n=== Updating TXT Resource ===")
    update_response = client.update_url(
        resource_id=resource_id,
        resource_name=updated_name,
        resource_title="Updated TXT Resource",
        processing=updated_processing,
        notes="This is an updated TXT resource for testing."
    )
    print(f"Updated TXT Resource Response: {update_response}")
    assert update_response.get("message") == "Resource updated successfully", "Failed to update TXT resource"

    # Verify the updated resource
    print("\n=== Verifying Updated TXT Resource ===")
    updated_resource = verify_resource_exists(client, search_term=updated_name, expected_name=updated_name)
    print(f"Updated Resource: {updated_resource}")
    assert updated_resource["extras"]["processing"]["delimiter"] == ",", "Processing field not updated correctly"
    
    # Ensure the resource is deleted at the end of the test
    print("\n=== Deleting TXT Resource ===")
    delete_response = client.delete_resource(resource_id)
    print(f"Deleted resource: {delete_response}")
    assert delete_response.get("message") == f"{resource_id} deleted successfully", "Failed to delete stream resource"

def test_json_url(client):
    """
    Test registering, retrieving, and updating a JSON URL resource.
    """
    resource_name = "json_resource_" + generate_unique_name()
    json_processing = JSONProcessing(info_key="count", data_key="results")
    
    # Register the resource
    print("\n=== Registering JSON Resource ===")
    response = client.register_url(
        resource_name=resource_name,
        resource_title="JSON Resource Example",
        owner_org=OWNER_ORG,
        resource_url="http://example.com/json",
        file_type="JSON",
        processing=json_processing,
        notes="This is a JSON resource for testing.",
        extras={"key1": "value1", "key2": "value2"}
    )
    print(f"Registered JSON Resource Response: {response}")
    resource_id = response.get("id")
    assert resource_id, "Failed to register JSON resource"

    # Retrieve and verify the resource
    print("\n=== Verifying Registered JSON Resource ===")
    resource = verify_resource_exists(client, search_term=resource_name, expected_name=resource_name)
    print(f"Retrieved Resource: {resource}")

    # Update the resource
    updated_name = "updated_json_resource_" + generate_unique_name()
    updated_processing = JSONProcessing(info_key="new_count", data_key="new_results")
    print("\n=== Updating JSON Resource ===")
    update_response = client.update_url(
        resource_id=resource_id,
        resource_name=updated_name,
        resource_title="Updated JSON Resource",
        processing=updated_processing,
        notes="This is an updated JSON resource for testing."
    )
    print(f"Updated JSON Resource Response: {update_response}")
    assert update_response.get("message") == "Resource updated successfully", "Failed to update JSON resource"

    # Verify the updated resource
    print("\n=== Verifying Updated JSON Resource ===")
    updated_resource = verify_resource_exists(client, search_term=updated_name, expected_name=updated_name)
    print(f"Updated Resource: {updated_resource}")
    assert updated_resource["extras"]["processing"]["info_key"] == "new_count", "Processing field not updated correctly"
    
        # Ensure the resource is deleted at the end of the test
    print("\n=== Deleting JSON Resource ===")
    delete_response = client.delete_resource(resource_id)
    print(f"Deleted resource: {delete_response}")
    assert delete_response.get("message") == f"{resource_id} deleted successfully", "Failed to delete stream resource"

def test_netcdf_url(client):
    """
    Test registering, retrieving, and updating a NetCDF URL resource.
    """
    resource_name = "netcdf_resource_" + generate_unique_name()
    netcdf_processing = NetCDFProcessing(group="example_group")
    
    # Register the resource
    print("\n=== Registering NetCDF Resource ===")
    response = client.register_url(
        resource_name=resource_name,
        resource_title="NetCDF Resource Example",
        owner_org=OWNER_ORG,
        resource_url="http://example.com/netcdf",
        file_type="NetCDF",
        processing=netcdf_processing,
        notes="This is a NetCDF resource for testing.",
        extras={"key1": "value1", "key2": "value2"}
    )
    print(f"Registered NetCDF Resource Response: {response}")
    resource_id = response.get("id")
    assert resource_id, "Failed to register NetCDF resource"

    # Retrieve and verify the resource
    print("\n=== Verifying Registered NetCDF Resource ===")
    resource = verify_resource_exists(client, search_term=resource_name, expected_name=resource_name)
    print(f"Retrieved Resource: {resource}")

    # Update the resource
    updated_name = "updated_netcdf_resource_" + generate_unique_name()
    updated_processing = NetCDFProcessing(group="new_example_group")
    print("\n=== Updating NetCDF Resource ===")
    update_response = client.update_url(
        resource_id=resource_id,
        resource_name=updated_name,
        resource_title="Updated NetCDF Resource",
        processing=updated_processing,
        notes="This is an updated NetCDF resource for testing."
    )
    print(f"Updated NetCDF Resource Response: {update_response}")
    assert update_response.get("message") == "Resource updated successfully", "Failed to update NetCDF resource"

    # Verify the updated resource
    print("\n=== Verifying Updated NetCDF Resource ===")
    updated_resource = verify_resource_exists(client, search_term=updated_name, expected_name=updated_name)
    print(f"Updated Resource: {updated_resource}")
    assert updated_resource["extras"]["processing"]["group"] == "new_example_group", "Processing field not updated correctly"
    
    # Ensure the resource is deleted at the end of the test
    print("\n=== Deleting NetCDF Resource ===")
    delete_response = client.delete_resource(resource_id)
    print(f"Deleted resource: {delete_response}")
    assert delete_response.get("message") == f"{resource_id} deleted successfully", "Failed to delete stream resource"

if __name__ == "__main__":
    pytest.main([__file__])
