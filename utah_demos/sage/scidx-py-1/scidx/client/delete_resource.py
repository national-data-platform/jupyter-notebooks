import requests

def delete_resource(self, resource_id: str) -> dict:
    """
    Delete a resource in the sciDX system.

    Parameters
    ----------
    resource_id : str
        The ID of the resource to be deleted.

    Returns
    -------
    dict
        A dictionary containing the success message.

    Raises
    ------
    Exception
        If there is an error deleting the resource.
    """
    url = f"{self.api_url}/resource/{resource_id}"
    headers = self._get_headers()
    
    response = requests.delete(url, headers=headers)
    
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Error deleting resource: {response.content.decode('utf-8')}")
