# search_datasources

Search for existing data sources in the sciDX system.

## Parameters

- `dataset_name` (str, optional): The name of the dataset to search for.
- `owner_org` (str, optional): The organization ID to search within.
- `search_term` (str, optional): A search term to filter the results.

## Returns

- `list`: A list of data sources that match the search criteria.

## Raises

- `Exception`: If there is an error retrieving the data sources.

## Example

```python
from scidx.client import sciDXClient

# Initialize the client with the base URL of the sciDX REST API
client = sciDXClient(api_url="http://example.com/api")

# Define the search parameters
search_params = {
    "dataset_name": "example_dataset",
    "owner_org": "org123",
    "search_term": "example"
}

# Search for data sources
response = client.search_datasources(**search_params)

print(response)
```

Return to [Usage](../usage.md)
