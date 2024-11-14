# login

Authenticate the user and obtain an access token.

## Parameters

- `username` (str): The username of the user.
- `password` (str): The password of the user.

## Raises

- `HTTPError`: If the authentication request fails.

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
    login=client.login(username, password)
    print(login)
```
\
\
Return to [Usage](../usage.md)