import requests
from typing import Optional, Union, Dict
from .register_url import StreamProcessing, CSVProcessing, TXTProcessing, JSONProcessing, NetCDFProcessing

def update_url(self, resource_id: str, resource_name: Optional[str] = None,
               resource_title: Optional[str] = None, owner_org: Optional[str] = None,
               resource_url: Optional[str] = None, file_type: Optional[str] = None,
               processing: Optional[Union[StreamProcessing, CSVProcessing, TXTProcessing, JSONProcessing, NetCDFProcessing]] = None,
               notes: Optional[str] = None, extras: Optional[Dict[str, str]] = None,
               mapping: Optional[Dict[str, str]] = None) -> dict:
    """
    Update an existing URL resource in the sciDX system.

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
    resource_url : str, optional
        The URL of the resource.
    file_type : str, optional
        The type of the file (e.g., stream, CSV, TXT, JSON, NetCDF).
    processing : Union[StreamProcessing, CSVProcessing, TXTProcessing, JSONProcessing, NetCDFProcessing], optional
        The processing information specific to the file type.
    notes : str, optional
        Additional notes about the resource.
    extras : dict, optional
        Additional metadata to be added to the resource.
    mapping : dict, optional
        Mapping information for the dataset.

    Returns
    -------
    dict
        A dictionary containing the response from the API.

    Raises
    ------
    HTTPError
        If the API request fails with detailed error information.

    Examples
    --------
    >>> client.update_url("resource_id", resource_name="updated_name")
    """
    url = f"{self.api_url}/url/{resource_id}"
    headers = self._get_headers()

    # Build the payload
    payload = {
        "resource_name": resource_name,
        "resource_title": resource_title,
        "owner_org": owner_org,
        "resource_url": resource_url,
        "file_type": file_type,
        "notes": notes,
        "extras": extras or {},
        "mapping": mapping or {},
        "processing": processing.to_dict() if processing else None,
    }

    # Remove keys with None values from the payload
    payload = {k: v for k, v in payload.items() if v is not None}

    response = requests.put(url, json=payload, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        error_message = (
            f"Failed to update URL resource. "
            f"Request URL: {url}\n"
            f"Payload: {payload}\n"
            f"Response Status Code: {response.status_code}\n"
            f"Response Content: {response.content.decode('utf-8')}"
        )
        raise requests.exceptions.HTTPError(error_message, response=response)
