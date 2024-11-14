# search_organization

Search for existing organizations in the sciDX system.

## Parameters

None.

## Returns

- `list`: A list of organizations.

## Raises

- `Exception`: If there is an error retrieving the organizations.

## Example

```python
from scidx.client import sciDXClient

# Initialize the client with the base URL of the sciDX REST API
client = sciDXClient(api_url="http://example.com/api")

# Search for organizations
response = client.search_organization()

print(response)
```

Return to [Usage](../usage.md)
