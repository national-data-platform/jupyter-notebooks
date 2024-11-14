# register_s3

Create a new S3 resource in the sciDX system.

## Parameters

- `resource_name` (str): The name of the resource.
- `resource_title` (str): The title of the resource.
- `owner_org` (str): The name of the organization.
- `resource_s3` (str): The S3 path of the resource.
- `notes` (str, optional): Additional notes about the resource (default is an empty string).
- `extras` (dict, optional): Additional metadata to be added to the resource.

## Returns

- `dict`: A dictionary containing the response from the API.

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
    print(f"Access token: {client.token}"

# Register a new S3 resource
resource_name = "example_resource"
resource_title = "Example Resource"
owner_org = "example_org"
resource_s3 = "s3://bucket/path/to/resource"
notes = "This is an example S3 resource."
extras = {"key1": "value1", "key2": "value2"}

try:
    response = client.register_s3(resource_name, resource_title, owner_org, resource_s3, notes, extras)
    print(response)
```
\
\
Return to [Usage](../usage.md)    
