from typing import Dict, Optional
import requests

def update_s3(self, resource_id: str, resource_name: Optional[str] = None,
              resource_title: Optional[str] = None, owner_org: Optional[str] = None,
              resource_s3: Optional[str] = None, notes: Optional[str] = None,
              extras: Optional[Dict[str, str]] = None) -> dict:
    """
    Update an existing S3 resource in the sciDX system.

    Parameters
    ----------
    resource_id : str
        The ID of the resource to update.
    resource_name : str, optional
        The name of the resource.
    resource_title : str, optional
        The title of the resource.
    owner_org : str, optional
        The name of the organization.
    resource_s3 : str, optional
        The S3 path of the resource.
    notes : str, optional
        Additional notes about the resource.
    extras : dict, optional
        Additional metadata to be added to the resource.

    Returns
    -------
    dict
        A dictionary containing the response from the API.

    Raises
    ------
    HTTPError
        If the API request fails with detailed error information.
    """
    url = f"{self.api_url}/s3/{resource_id}"
    headers = self._get_headers()
    payload = {
        "resource_name": resource_name,
        "resource_title": resource_title,
        "owner_org": owner_org,
        "resource_s3": resource_s3,
        "notes": notes,
        "extras": extras or {}
    }
    
    # Remove keys with None values from the payload
    payload = {k: v for k, v in payload.items() if v is not None}
    
    response = requests.put(url, json=payload, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        error_message = (
            f"Failed to update S3 resource. "
            f"Request URL: {url}\n"
            f"Payload: {payload}\n"
            f"Response Status Code: {response.status_code}\n"
            f"Response Content: {response.content.decode('utf-8')}"
        )
        raise requests.exceptions.HTTPError(error_message, response=response)
