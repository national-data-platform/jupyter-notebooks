import requests

def login(self, username: str, password: str) -> None:
    """
    Authenticate the user and obtain an access token.

    Parameters
    ----------
    username : str
        The username of the user.
    password : str
        The password of the user.

    Raises
    ------
    HTTPError
        If the authentication request fails.
    """
    url = f"{self.api_url}/token"
    response = requests.post(url, data={"grant_type": "password", "username": username, "password": password}, headers = {"Content-Type": "application/x-www-form-urlencoded"})
    if response.status_code == 200:
        self.token = response.json()["access_token"]
    else:
        error_message = (
            f"Failed to authenticate user. "
            f"Request URL: {url}\n"
            f"Response Status Code: {response.status_code}\n"
            f"Response Content: {response.content.decode('utf-8')}"
        )
        raise requests.exceptions.HTTPError(error_message, response=response)
