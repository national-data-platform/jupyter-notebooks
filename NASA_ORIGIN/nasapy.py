# nasapy.py

import requests
from ckanapi import RemoteCKAN
from datetime import datetime
import os

def convert_to_datetime(datetime_str):
    """
    Convert CKAN datetime string to datetime object.
    """
    try:
        return datetime.strptime(datetime_str, '%Y-%m-%dT%H:%M:%S.%f')
    except ValueError:
        return datetime.strptime(datetime_str, '%Y-%m-%dT%H:%M:%S')

def print_dataset_details(dataset):
    """
    Print dataset details including title and links.
    """
    print(f"{'='*40}")
    print(f"Dataset Title: {dataset['title']}")
    s3_link = None
    pelican_origin = None
    for extra in dataset['extras']:
        if extra['key'] == 's3_link':
            s3_link = extra['value']
        elif extra['key'] == 'pelican_origin':
            pelican_origin = extra['value']
    if s3_link:
        print(f"S3 Link: {s3_link}")
    if pelican_origin:
        print(f"Pelican Origin: {pelican_origin}")
    print(f"{'='*40}")

def search_datasets_by_title(ckan_instance_url, title):
    """
    Search for datasets by title.
    """
    ckan = RemoteCKAN(ckan_instance_url)
    search_params = {
        'q': f'title:{title}'
    }
    response = ckan.action.package_search(**search_params)
    return response['results']

def search_datasets_by_start_time(ckan_instance_url, start_time):
    """
    Search for datasets by start time.
    """
    ckan = RemoteCKAN(ckan_instance_url)
    search_params = {
        'q': 'observation_start_datetime',
        'rows': 100000
    }
    response = ckan.action.package_search(**search_params)
    datasets = response['results']
    return [dataset for dataset in datasets if any(extra['key'] == 'observation_start_datetime' and extra['value'] == start_time for extra in dataset['extras'])]

def get_matching_datasets(ckan_instance_url, specific_dataset_title):
    """
    Get the specific dataset by title and find matching datasets with the same start time.
    """
    datasets = search_datasets_by_title(ckan_instance_url, specific_dataset_title)
    if not datasets:
        print(f"Dataset with title '{specific_dataset_title}' not found.")
        return

    specific_dataset = datasets[0]
    start_time = None
    for extra in specific_dataset['extras']:
        if extra['key'] == 'observation_start_datetime':
            start_time = extra['value']
            break

    if not start_time:
        print(f"Start time not found for the dataset with title '{specific_dataset_title}'.")
        return

    print("Details of the specified dataset:", specific_dataset_title, "\n")
    print_dataset_details(specific_dataset)

    matching_datasets = search_datasets_by_start_time(ckan_instance_url, start_time)
    print("\nMatching datasets with the same start time:\n")
    for dataset in matching_datasets:
        if dataset['title'] != specific_dataset_title:
            print_dataset_details(dataset)

def filter_datasets_by_time_range(ckan_instance_url, start_time, end_time, query='nasa', rows=100000):
    """
    Filter datasets based on user-provided start and end time.
    """
    ckan = RemoteCKAN(ckan_instance_url)
    search_params = {
        'q': query,
        'rows': rows
    }
    response = ckan.action.package_search(**search_params)
    datasets = response['results']

    filtered_datasets = []
    for dataset in datasets:
        start_time_ds = None
        end_time_ds = None
        for extra in dataset['extras']:
            if extra['key'] == 'observation_start_datetime':
                start_time_ds = convert_to_datetime(extra['value'])
            if extra['key'] == 'observation_end_datetime':
                end_time_ds = convert_to_datetime(extra['value'])
        
        if start_time_ds and end_time_ds and start_time <= start_time_ds <= end_time and start_time <= end_time_ds <= end_time:
            filtered_datasets.append(dataset)

    return filtered_datasets

def print_filtered_datasets(ckan_instance_url, start_time, end_time, query='nasa', rows=1000):
    """
    Search and print filtered datasets based on user-provided start and end time.
    """
    filtered_datasets = filter_datasets_by_time_range(ckan_instance_url, start_time, end_time, query, rows)
    for dataset in filtered_datasets:
        print(f"{'='*40}")
        print(f"Dataset Title: {dataset['title']}")
        print(f"Dataset ID: {dataset['id']}")
        print(f"{'-'*40}")
        print("Attributes:")
        for key, value in dataset.items():
            if key != 'extras':
                print(f"  {key}: {value}")
        print("Extras:")
        for extra in dataset['extras']:
            print(f"  {extra['key']}: {extra['value']}")
        print(f"{'='*40}")

    all_keys = set()
    for dataset in filtered_datasets:
        all_keys.update(dataset.keys())

    print("All Dataset Attributes:")
    for key in all_keys:
        print(key)

def ensure_directory_exists(directory):
    """
    Ensure that the specified directory exists.
    """
    if not os.path.exists(directory):
        os.makedirs(directory)

def download_file(url, local_filename):
    """
    Download file from the given URL and save it locally.
    """
    ensure_directory_exists(os.path.dirname(local_filename))
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

def download_matching_files(ckan_instance_url, specific_dataset_title, download_directory):
    """
    Download the radc file and the matching fdcc file from either the pelican origin or s3.
    Return the paths of the downloaded files.
    """
    datasets = search_datasets_by_title(ckan_instance_url, specific_dataset_title)
    if not datasets:
        print(f"Dataset with title '{specific_dataset_title}' not found.")
        return None, None

    specific_dataset = datasets[0]
    start_time = None
    for extra in specific_dataset['extras']:
        if extra['key'] == 'observation_start_datetime':
            start_time = extra['value']
            break

    if not start_time:
        print(f"Start time not found for the dataset with title '{specific_dataset_title}'.")
        return None, None

    matching_datasets = search_datasets_by_start_time(ckan_instance_url, start_time)
    radc_path = None
    fdcc_path = None

    for dataset in matching_datasets:
        title = dataset['title']
        s3_link = None
        pelican_origin = None
        for extra in dataset['extras']:
            if extra['key'] == 's3_link':
                s3_link = extra['value']
            elif extra['key'] == 'pelican_origin':
                pelican_origin = extra['value']

        if 'radc' in title.lower() and radc_path is None:
            try:
                print(f"Downloading RADC file: {title}")
                file_path = os.path.join(download_directory, title)
                if pelican_origin:
                    download_file(pelican_origin, file_path)
                else:
                    download_file(s3_link, file_path)
                radc_path = file_path
            except Exception as e:
                print(f"Failed to download RADC file from Pelican Origin. Error: {e}")
                if s3_link:
                    print("Attempting to download from S3...")
                    try:
                        download_file(s3_link, file_path)
                        radc_path = file_path
                    except Exception as e:
                        print(f"Failed to download RADC file from S3. Error: {e}")

        if 'fdcc' in title.lower() and fdcc_path is None:
            try:
                print(f"Downloading FDCC file: {title}")
                file_path = os.path.join(download_directory, title)
                if pelican_origin:
                    download_file(pelican_origin, file_path)
                else:
                    download_file(s3_link, file_path)
                fdcc_path = file_path
            except Exception as e:
                print(f"Failed to download FDCC file from Pelican Origin. Error: {e}")
                if s3_link:
                    print("Attempting to download from S3...")
                    try:
                        download_file(s3_link, file_path)
                        fdcc_path = file_path
                    except Exception as e:
                        print(f"Failed to download FDCC file from S3. Error: {e}")

    if radc_path is None:
        print("No RADC file found to download.")
    if fdcc_path is None:
        print("No FDCC file found to download.")

    return radc_path, fdcc_path
# Example usage:
# ckan_instance_url = 'https://ckan.geosciframe.org:8443'
# specific_dataset_title = 'OR_ABI-L1b-RadC-M6C16_G18_s20231330001191_e20231330003578_c20231330004074.nc'
# get_matching_datasets(ckan_instance_url, specific_dataset_title)

# Example usage for filtering datasets:
# user_start_time = datetime(2023, 5, 13, 0, 0, 0)
# user_end_time = datetime(2023, 5, 13, 0, 5, 0)
# print_filtered_datasets(ckan_instance_url, user_start_time, user_end_time)

# Example usage for downloading files:
# download_directory = '/path/to/download/directory'
# download_matching_files(ckan_instance_url, specific_dataset_title, download_directory)
