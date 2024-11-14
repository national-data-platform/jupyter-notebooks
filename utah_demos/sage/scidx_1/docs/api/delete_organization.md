# delete_organization

Delete an existing organization in the sciDX system.

## Parameters

- `organization_name` (str): The name of the organization to delete.

## Returns

- `dict`: A dictionary containing a success message.

## Raises

- `Exception`: If there is an error deleting the organization.

## Example

```python
from scidx.client import sciDXClient

# Initialize the client with the base URL of the sciDX REST API
client = sciDXClient(api_url="http://example.com/api")

# Define the organization name to delete
organization_name = "example_organization"

# Delete the organization
response = client.delete_organization(organization_name)

print(response)
```

Return to [Usage](../usage.md)
