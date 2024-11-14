class sciDXClient:
    """
    A client to interact with the sciDX REST API.

    Parameters
    ----------
    api_url : str
        The base URL of the sciDX REST API.

    Attributes
    ----------
    api_url : str
        The base URL of the sciDX REST API.
    token : str
        The authentication token.
    """
    
    def __init__(self, api_url: str):
        """
        Initialize the sciDXClient with the API URL.

        Parameters
        ----------
        api_url : str
            The base URL of the external API.
        """
        self.api_url = api_url
        self.token = None

    def _get_headers(self) -> dict:
        """
        Get the headers for the API request.

        Returns
        -------
        dict
            A dictionary containing the headers.
        """
        if self.token:
            return {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}
        return {"Content-Type": "application/json"}
