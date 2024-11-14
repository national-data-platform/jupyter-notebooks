import pytest
import uuid
from scidx import sciDXClient

# Function to generate a unique organization name
def generate_unique_name(base_name):
    return f"{base_name}_{uuid.uuid4().hex[:6]}"

# Global variables to store IDs and names
api_url = "http://127.0.0.1:8000"
USERNAME = "placeholder@placeholder.com"
PASSWORD = "placeholder"

@pytest.fixture(scope="module", autouse=True)
def setup_and_cleanup():
    global client, organization_name, organization_data

    # Initialize client and login to get the token
    client = sciDXClient(api_url)
    client.login(USERNAME, PASSWORD)
    token = client.token

    # Create organization data with token
    organization_name = generate_unique_name("pytest_organization")
    organization_data = {
        "name": organization_name,
        "title": "Pytest Organization",
        "api_token": token,
        "description": "Organization created for pytest."
    }

    # Setup: Ensure the organization does not already exist
    print("Setup: Checking if the organization already exists")
    existing_orgs = client.search_organization()
    print(f"Setup: Existing organizations: {existing_orgs}")

    # Run the tests
    yield

    # Cleanup: Delete the organization after tests
    # try:
    #     client.delete_organization(organization_name)
    #     print(f"Cleanup: Deleted organization {organization_name}")
    # except Exception as e:
    #     print(f"Cleanup: Failed to delete organization {organization_name}. Reason: {str(e)}")

# Test to create an organization
@pytest.mark.order(1)
def test_create_organization():
    global client, organization_data
    response = client.register_organization(
        name=organization_data["name"],
        title=organization_data["title"],
        description=organization_data["description"]
    )
    assert "id" in response
    assert response["message"] == "Organization created successfully"
