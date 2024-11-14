# register_kafka

Create a new Kafka dataset in the sciDX system.

## Parameters

- `dataset_name` (str): The name of the dataset.
- `dataset_title` (str): The title of the dataset.
- `owner_org` (str): The name of the organization.
- `kafka_topic` (str): The Kafka topic name.
- `kafka_host` (str): The Kafka host address.
- `kafka_port` (int): The Kafka port number.
- `dataset_description` (str, optional): The description of the dataset (default is an empty string).
- `extras` (dict, optional): Additional metadata to be added to the dataset.

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
    print(f"Access token: {client.token}")


# Register a new Kafka dataset
dataset_name = "example_dataset"
dataset_title = "Example Dataset"
owner_org = "example_org"
kafka_topic = "example_topic"
kafka_host = "localhost"
kafka_port = 9092
dataset_description = "This is an example Kafka dataset."
extras = {"key1": "value1", "key2": "value2"}

try:
    response = client.register_kafka(dataset_name, dataset_title, owner_org, kafka_topic, kafka_host, kafka_port, dataset_description, extras)
    print(response)
```
\
\
Return to [Usage](../usage.md)