import requests
from requests.exceptions import HTTPError

def get_api_token(self, username: str, password: str) -> str:
    """
    Retrieve an access token from the API using the provided username and password.

    Parameters
    ----------
    username : str
        The username for authentication.
    password : str
        The password for authentication.

    Returns
    -------
    str
        The access token.

    Raises
    ------
    HTTPError
        If the API request fails with detailed error information.
    """
    auth_url = f"{self.api_url}/token/"
    payload = {
        'username': username,
        'password': password
    }

    headers = {
        'accept': 'application/json',
        'Content-Type': 'application/x-www-form-urlencoded'
    }

    try:
        response = requests.post(auth_url, data=payload, headers=headers)
        response.raise_for_status()

        # Debug prints
        # print(f"Request URL: {auth_url}")
        # print(f"Payload: {payload}")
        # print(f"Response Status Code: {response.status_code}")
        # print(f"Response Content: {response.content.decode('utf-8')}")

        token_data = response.json()
        return token_data.get('access_token')

    except HTTPError as http_err:
        error_message = (
            f"Failed to retrieve API token. "
            f"Request URL: {auth_url}\n"
            f"Payload: {payload}\n"
            f"Response Status Code: {response.status_code}\n"
            f"Response Content: {response.content.decode('utf-8')}\n"
            f"HTTP Error: {http_err}"
        )
        print(error_message)
        raise
    except Exception as err:
        print(f"An error occurred: {err}")
        raise
