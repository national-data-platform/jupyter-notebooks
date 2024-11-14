# search_resource

Search for resources in the sciDX system by various parameters.

## Parameters

- `dataset_name` (Optional[str]): The name of the dataset.
- `dataset_title` (Optional[str]): The title of the dataset.
- `owner_org` (Optional[str]): The name of the organization.
- `resource_url` (Optional[str]): The URL of the dataset resource.
- `resource_name` (Optional[str]): The name of the dataset resource.
- `dataset_description` (Optional[str]): The description of the dataset.
- `resource_description` (Optional[str]): The description of the dataset resource.
- `resource_format` (Optional[str]): The format of the dataset resource.
- `search_term` (Optional[str]): A term to search across all fields.

## Returns

- `List[dict]`: A list of datasets that match the search criteria.

## Raises

- `HTTPError`: If the API request fails with detailed error information.

## Example

```python
# Import the necessary function
from scidx.client import sciDXClient

# Set the API URL, username, and password
api_url = "http://example.com/api"
username = "your_username"
password = "your_password"
client = sciDXClient(api_url="http://example.com/api")

# Authenticate the user
try:
    client.login(username, password)
    print(f"respone: {client.token}")


# Search for resources
search_params = {
    "dataset_name": "example_dataset",
    "dataset_title": "Example Dataset",
    "owner_org": "example_org",
    "resource_name": "example_resource"
}

try:
    results = client.search_resource(**search_params)
    print(results)
```
\
\
Return to [Usage](../usage.md)    