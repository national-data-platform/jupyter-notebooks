import requests
from typing import Optional, Dict, Any

def update_kafka(self, resource_id: str, dataset_name: Optional[str] = None,
                 dataset_title: Optional[str] = None, owner_org: Optional[str] = None,
                 kafka_topic: Optional[str] = None, kafka_host: Optional[str] = None,
                 kafka_port: Optional[Any] = None, dataset_description: Optional[str] = None,
                 extras: Optional[Dict[str, str]] = None, mapping: Optional[Dict[str, str]] = None,
                 processing: Optional[Dict[str, str]] = None) -> dict:
    """
    Update an existing Kafka dataset in the sciDX system.

    Parameters
    ----------
    resource_id : str
        The ID of the dataset to update.
    dataset_name : str, optional
        The name of the dataset.
    dataset_title : str, optional
        The title of the dataset.
    owner_org : str, optional
        The name of the organization.
    kafka_topic : str, optional
        The Kafka topic name.
    kafka_host : str, optional
        The Kafka host.
    kafka_port : int, optional
        The Kafka port.
    dataset_description : str, optional
        A description of the dataset.
    extras : dict, optional
        Additional metadata to be added or updated for the dataset.
    mapping : dict, optional
        Mapping information for the dataset.
    processing : dict, optional
        Processing information for the dataset.

    Returns
    -------
    dict
        A dictionary containing the response from the API.

    Raises
    ------
    HTTPError
        If the API request fails with detailed error information.
    """
    url = f"{self.api_url}/kafka/{resource_id}"
    headers = self._get_headers()
    payload = {
        "dataset_name": dataset_name,
        "dataset_title": dataset_title,
        "owner_org": owner_org,
        "kafka_topic": kafka_topic,
        "kafka_host": kafka_host,
        "kafka_port": str(kafka_port) if kafka_port is not None else None,
        "dataset_description": dataset_description,
        "extras": extras or {},
        "mapping": mapping,
        "processing": processing
    }
    
    # Remove keys with None values from the payload
    payload = {k: v for k, v in payload.items() if v is not None}
    
    response = requests.put(url, json=payload, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        error_message = (
            f"Failed to update Kafka dataset. "
            f"Request URL: {url}\n"
            f"Payload: {payload}\n"
            f"Response Status Code: {response.status_code}\n"
            f"Response Content: {response.content.decode('utf-8')}"
        )
        raise requests.exceptions.HTTPError(error_message, response=response)
