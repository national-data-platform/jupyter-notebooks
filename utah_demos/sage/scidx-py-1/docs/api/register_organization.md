# register_organization

Register a new organization in the sciDX system.

## Parameters

- `name` (str): The name of the organization.
- `title` (str): The title of the organization.
- `description` (str): A description of the organization.

## Returns

- `dict`: A dictionary containing the ID of the created organization and a success message.

## Raises

- `Exception`: If there is an error creating the organization.

## Example

```python
from scidx.client import sciDXClient

# Initialize the client with the base URL of the sciDX REST API
client = sciDXClient(api_url="http://example.com/api")

# Define the parameters for the new organization
organization_data = {
    "name": "example_organization",
    "title": "Example Organization",
    "description": "This is an example organization."
}

# Register the organization
response = client.register_organization(**organization_data)

print(response)
```

Return to [Usage](../usage.md)
