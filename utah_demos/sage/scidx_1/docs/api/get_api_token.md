# get_api_token

Retrieve an access token from the API using the provided username and password.

## Parameters

- `username` (str): The username for authentication.
- `password` (str): The password for authentication.

## Returns

- `str`: The access token.

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
# Retrieve the access token
try:
    token = client.get_api_token(api_url, username, password)
    print(token)
```
\
\
Return to [Usage](../usage.md)