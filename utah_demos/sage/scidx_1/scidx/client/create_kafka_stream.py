import requests

def create_kafka_stream(self, keywords: list, filter_semantics: dict, match_all: bool = False) -> dict:
    """
    Create a new Kafka stream in the sciDX system.

    Parameters
    ----------
    keywords : list
        A list of keywords to filter the data sources.
    filter_semantics : dict
        A dictionary containing filter semantics to apply to the streams.
    match_all : bool, optional
        If True, all keywords must match the stream (default is False).

    Returns
    -------
    dict
        A dictionary containing the response from the API, including the topic of the created stream.

    Raises
    ------
    HTTPError
        If the API request fails with detailed error information.
    """
    url = f"{self.api_url}/stream"
    
    # Convert the list of keywords to a single comma-separated string
    keywords_str = ','.join(keywords)
    payload = {
        "keywords": keywords_str,
        "filter_semantics": filter_semantics,
        "match_all": match_all
    }
    headers = self._get_headers()
    
    try:
        response = requests.post(url, json=payload, headers=headers, timeout=10)
        response.raise_for_status()  # Raise an exception for any 4xx/5xx response
        return response.json()
    
    except requests.exceptions.Timeout:
        error_message = (
            f"Request timed out when creating Kafka stream. "
            f"Request URL: {url}\nPayload: {payload}\n"
        )
        raise requests.exceptions.Timeout(error_message)
    
    except requests.exceptions.HTTPError as http_err:
        error_message = (
            f"HTTP error occurred: {http_err}. "
            f"Request URL: {url}\nPayload: {payload}\n"
            f"Response Status Code: {response.status_code}\n"
            f"Response Content: {response.content.decode('utf-8')}"
        )
        raise requests.exceptions.HTTPError(error_message)
    
    except Exception as e:
        error_message = f"An error occurred while creating Kafka stream: {str(e)}"
        raise requests.exceptions.RequestException(error_message)
