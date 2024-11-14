# logout

Log out the user by clearing the access token. you need to be logged in first 

## Example

```python
# Import the necessary function
from scidx.client import sciDXClient

# Set the API URL, username, and password

client = sciDXClient(api_url="http://example.com/api")

# Log out the user
client.logout()
print("User logged out successfully.")
```
\
\
Return to [Usage](../usage.md)