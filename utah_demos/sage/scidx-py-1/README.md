# scidx

scidx is a Python library that allows you to interact with the sciDX REST API for managing datasets and organizations. This library provides a convenient way to create, search, and manage datasets and organizations using simple Python functions.

## Features

- Register organizations
- Register URL resources
- Register S3 resources
- Register Kafka resources
- Search organizations
- Search resources

## Installation

To install the library, you can use pip:

```
pip install scidx
```

For detailed installation instructions and dependencies, see [installation](https://github.com/sci-ndp/scidx-py/blob/main/docs/installation.md).

## Configuration

To configure the library, you need to set the API URL for your sciDX REST API instance. This can be done by initializing the `sciDXClient` with the appropriate URL:

```python
from scidx.client import sciDXClient

api_url = "http://your-api-url.com"
client = sciDXClient(api_url)
```

For detailed configuration instructions, see [Configuration](https://github.com/sci-ndp/scidx-py/blob/main/docs/configuration.md).

## Usage

Here is a quick example of how to use the library:

```python
from scidx import sciDXClient

# Initialize the client
api_url = "http://your-api-url.com"
client = sciDXClient(api_url)

# Register an organization
response = client.register_organization(
    name="example_org",
    title="Example Organization",
    description="This is an example organization."
)
print(response)

# Register a URL resource
response = client.register_url(
    resource_name="example_resource",
    resource_title="Example Resource Title",
    owner_org="example_org",
    resource_url="http://example.com/resource",
    notes="This is a resource for testing."
)
print(response)

# Register an S3 resource
response = client.register_s3(
    resource_name="example_resource",
    resource_title="Example Resource Title",
    owner_org="example_org",
    resource_s3="s3://example-bucket/resource",
    notes="This is a resource for testing."
)
print(response)

# Search for organizations
organizations = client.search_organization()
print(organizations)

# Search for resources
resources = client.search_resource(resource_name="example_resource")
print(resources)
```

For more usage examples and detailed explanations, see [Usage](https://github.com/sci-ndp/scidx-py/blob/main/docs/usage.md).

## Testing

To run the tests for this project, you can use pytest:

```bash
pytest
```

For detailed testing instructions, see [Testing](https://github.com/sci-ndp/scidx-py/blob/main/docs/testing.md).

## Contributing

We welcome contributions to the sciDX-PY project. To contribute, please follow the guidelines in [Contributing](https://github.com/sci-ndp/scidx-py/blob/main/docs/contributing.md).

## License

This project is licensed under the MIT License. See [LICENSE.md](https://github.com/sci-ndp/scidx-py/blob/main/docs/LICENSE.md) for more details.

## Contact

For any questions or suggestions, please open an [issue](https://github.com/sci-ndp/scidx-py/blob/main/docs/issues.md) on GitHub.
