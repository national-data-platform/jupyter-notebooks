import requests
from typing import Optional, List


def search_resource(self, dataset_name: Optional[str] = None, dataset_title: Optional[str] = None,
                    owner_org: Optional[str] = None, resource_url: Optional[str] = None,
                    resource_name: Optional[str] = None, dataset_description: Optional[str] = None,
                    resource_description: Optional[str] = None, resource_format: Optional[str] = None,
                    search_term: Optional[str] = None) -> List[dict]:
    """
    Search for resources in the sciDX system by various parameters.

    Parameters
    ----------
    dataset_name : Optional[str]
        The name of the dataset.
    dataset_title : Optional[str]
        The title of the dataset.
    owner_org : Optional[str]
        The name of the organization.
    resource_url : Optional[str]
        The URL of the dataset resource.
    resource_name : Optional[str]
        The name of the dataset resource.
    dataset_description : Optional[str]
        The description of the dataset.
    resource_description : Optional[str]
        The description of the dataset resource.
    resource_format : Optional[str]
        The format of the dataset resource.
    search_term : Optional[str]
        A term to search across all fields.

    Returns
    -------
    List[dict]
        A list of datasets that match the search criteria.

    Raises
    ------
    HTTPError
        If the API request fails with detailed error information.
    """
    url = f"{self.api_url}/search"
    params = {
        "dataset_name": dataset_name,
        "dataset_title": dataset_title,
        "owner_org": owner_org,
        "resource_url": resource_url,
        "resource_name": resource_name,
        "dataset_description": dataset_description,
        "resource_description": resource_description,
        "resource_format": resource_format.lower() if resource_format else None,
        "search_term": search_term
    }
    # Remove None values from params
    params = {k: v for k, v in params.items() if v is not None}

    response = requests.get(url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        error_message = (
            f"Failed to search resources. "
            f"Request URL: {url}\n"
            f"Params: {params}\n"
            f"Response Status Code: {response.status_code}\n"
            f"Response Content: {response.content.decode('utf-8')}"
        )
        raise requests.exceptions.HTTPError(error_message, response=response)
