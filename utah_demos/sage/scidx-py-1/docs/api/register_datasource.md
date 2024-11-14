# register_datasource

Register a new data source in the sciDX system.

## Parameters

- `dataset_name` (str): The name of the dataset.
- `dataset_title` (str): The title of the dataset.
- `owner_org` (str): The ID of the organization.
- `resource_url` (str): The URL of the resource.
- `resource_name` (str): The name of the resource.
- `dataset_description` (str): The description of the dataset.
- `resource_description` (str): The description of the resource.
- `resource_format` (str): The format of the resource.

## Returns

- `dict`: A dictionary containing the ID of the created dataset.

## Raises

- `Exception`: If there is an error creating the dataset.

## Example

```python
from scidx.client import sciDXClient

# Initialize the client with the base URL of the sciDX REST API
client = sciDXClient(api_url="http://example.com/api")

# Define the parameters for the new data source
dataset_data = {
    "dataset_name": "test_dataset",
    "dataset_title": "Test Dataset",
    "owner_org": "org123",
    "resource_url": "http://example.com/resource",
    "resource_name": "Test Resource",
    "dataset_description": "A test dataset",
    "resource_description": "A test resource",
    "resource_format": "csv"
}

# Register the data source
response = client.register_datasource(**dataset_data)

print(response)
```

Return to [Usage](../usage.md)
