# ndpearthscope.py

import os
import requests
import folium
import matplotlib.pyplot as plt
from IPython.display import clear_output
from kafka import KafkaConsumer
import json
import collections
from mpl_toolkits.mplot3d import Axes3D
from IPython.display import clear_output
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.svm import OneClassSVM



CKAN_URL = 'https://ip-155-101-6-193.chpc.utah.edu:8443/'

def dataset_detail(dataset_name):
    """Get details of a specific dataset by name."""
    try:
        response = requests.get(f"{CKAN_URL}/api/3/action/package_show", params={"id": dataset_name})
        response.raise_for_status()
        data = response.json()
        if 'result' not in data:
            print(f"'result' key not found in response. Data: {data}")
            return None
        return data
    except requests.RequestException as e:
        print(f"Request failed: {e}")
        return None

def extract_specific_details(data):
    """Extract bootstrap_server, topic, file URL, latitude, and longitude from dataset details."""
    if data is None:
        return None, None, None, None, None

    bootstrap_server = None
    topic = None
    file_url = data.get('result', {}).get('url', None)
    latitude = None
    longitude = None

    extras = data.get('result', {}).get('extras', [])
    for item in extras:
        if item['key'] == 'bootstrap_server':
            bootstrap_server = item['value']
        elif item['key'] == 'Topic':
            topic = item['value']
        elif item['key'] == 'latitude':
            latitude = item['value']
        elif item['key'] == 'longitude':
            longitude = item['value']

    return bootstrap_server, topic, file_url, latitude, longitude

def extract_details_and_download(dataset_name):
    """Extract details and download the file for a specific dataset, including latitude and longitude."""
    details = dataset_detail(dataset_name)
    bootstrap_server, topic, file_url, latitude, longitude = extract_specific_details(details)

    if not file_url:
        print("No file URL found.")
        return None, bootstrap_server, topic, latitude, longitude

    try:
        print(f"Downloading file from {file_url}")
        response = requests.get(file_url)
        response.raise_for_status()

        file_path = os.path.basename(file_url)
        with open(file_path, 'wb') as file:
            file.write(response.content)
        print(f"File downloaded and saved as {file_path}")
        return file_path, bootstrap_server, topic, latitude, longitude
    except requests.RequestException as e:
        print(f"Failed to download the file. Error: {e}")
        return None, bootstrap_server, topic, latitude, longitude

def process_datasets(dataset_names):
    """Process multiple datasets by extracting details and downloading files.
    
    Returns a list of dictionaries with details for each dataset.
    """
    datasets_details = []
    for dataset_name in dataset_names:
        print(f"Processing dataset: {dataset_name}")
        file_path, bootstrap_server, topic, latitude, longitude = extract_details_and_download(dataset_name)
        dataset_detail = {
            "dataset_name": dataset_name,
            "file_path": file_path,
            "bootstrap_server": bootstrap_server,
            "topic": topic,
            "latitude": latitude,
            "longitude": longitude
        }
        datasets_details.append(dataset_detail)
        print(f"Details for {dataset_name}: {dataset_detail}")
        print("--------------------------------------------------")
    return datasets_details


def plot_station_location(latitude, longitude, station_name):
    """Plot station location on a map given latitude, longitude, and station name."""
    # Convert latitude and longitude to float if they are strings
    latitude = float(latitude)
    longitude = float(longitude)

    # Create a map centered around the coordinates
    station_map = folium.Map(location=[latitude, longitude], zoom_start=12)

    # Add a marker for the station with a popup showing the station name, latitude, and longitude
    popup_text = f"{station_name}<br>Latitude: {latitude}<br>Longitude: {longitude}"
    folium.Marker([latitude, longitude], popup=popup_text,
                  icon=folium.Icon(color='red', icon='info-sign')).add_to(station_map)

    # Return the map object
    return station_map


def consume_and_plot_kafka_data(topic, bootstrap_server):
    """
    Consumes data from a Kafka topic and updates a plot in real-time with the last 60 values.
    
    Parameters:
    - topic: The Kafka topic to subscribe to.
    - bootstrap_server: The bootstrap server for the Kafka cluster.
    """
    # Initialize Kafka consumer
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=[bootstrap_server],
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    # Function to update plot
    def update_plot(x_vals, y_vals, z_vals):
        # Keep only the last 60 values
        x_vals = x_vals[-60:]
        y_vals = y_vals[-60:]
        z_vals = z_vals[-60:]

        plt.figure(figsize=(10, 5))
        plt.plot(x_vals, label='X', color='red')
        plt.plot(y_vals, label='Y', color='green')
        plt.plot(z_vals, label='Z', color='blue')
        plt.xlabel('Time')
        plt.ylabel('Values')
        plt.title('Real-time Data from Kafka (Last 60 values)')
        plt.legend(loc='upper right')
        plt.show()

    # Lists to hold data
    x_vals = []
    y_vals = []
    z_vals = []

    # Process messages
    try:
        for message in consumer:
            message_value = message.value

            # Extract values
            x = message_value.get('x', 0)
            y = message_value.get('y', 0)
            z = message_value.get('z', 0)

            # Append to lists
            x_vals.append(x)
            y_vals.append(y)
            z_vals.append(z)

            # Update plot
            clear_output(wait=True)
            update_plot(x_vals, y_vals, z_vals)

    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        consumer.close()

def consume_and_plot_kafka_data_3d(topic, bootstrap_server):
    """
    Consumes data from a Kafka topic and updates a 3D plot in real-time with the last 60 values.

    Parameters:
    - topic: The Kafka topic to subscribe to.
    - bootstrap_server: The bootstrap server for the Kafka cluster.
    """
    # Initialize Kafka consumer
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=[bootstrap_server],
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    # Initialize deque with fixed size for efficient memory management
    x_vals = collections.deque(maxlen=60)
    y_vals = collections.deque(maxlen=60)
    z_vals = collections.deque(maxlen=60)

    def update_plot_3d(x_vals, y_vals, z_vals):
        """Updates the 3D plot with new data."""
        fig = plt.figure(figsize=(10, 7))
        ax = fig.add_subplot(111, projection='3d')

        # Convert deques to lists to use slicing
        x_vals_list = list(x_vals)
        y_vals_list = list(y_vals)
        z_vals_list = list(z_vals)

        # Plot the entire path in blue
        ax.plot(x_vals_list, y_vals_list, z_vals_list, label='GPS Path', color='blue')

        # If there are at least 2 points, plot the last segment in red
        if len(x_vals_list) > 1:
            ax.plot(x_vals_list[-2:], y_vals_list[-2:], z_vals_list[-2:], color='red', linewidth=2, label='Latest Movement')

        # Mark the latest position with a distinct color
        ax.scatter(x_vals_list[-1], y_vals_list[-1], z_vals_list[-1], color='red', s=50)  # Increased size for visibility

        ax.set_xlabel('East-West')
        ax.set_ylabel('North-South')
        ax.set_zlabel('Up-Down')
        plt.title('3D Real-time GPS Data Visualization')
        plt.legend()

        plt.show()

    # Process messages
    try:
        for message in consumer:
            message_value = message.value

            # Extract values
            x = float(message_value['x'])
            y = float(message_value['y'])
            z = float(message_value['z'])

            # Append to lists
            x_vals.append(x)
            y_vals.append(y)
            z_vals.append(z)

            # Update plot
            clear_output(wait=True)
            update_plot_3d(x_vals, y_vals, z_vals)

    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        consumer.close()

        
def detect_and_visualize_anomalies(file_path, nrows, nu, separator='\t'):
    """
    Detects and visualizes anomalies in a 3D dataset using One-Class SVM.
    
    Parameters:
    - file_path: Path to the dataset file.
    - separator: The delimiter string (default is tab-separated).
    - nrows: Number of rows of file to read (default is 12000).
    - nu: An upper bound on the fraction of training errors and a lower bound of the fraction of support vectors.
    """
    # Load data from text file
    df = pd.read_csv(file_path, sep=separator, nrows=nrows)

    # Ensure DataFrame has the necessary columns: 'x', 'y', 'z'
    if not set(['x', 'y', 'z']).issubset(df.columns):
        raise ValueError("DataFrame must contain 'x', 'y', 'z' columns")

    # Selecting features
    X = df[['x', 'y', 'z']]

    # Normalize features
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # Initialize One-Class SVM
    oc_svm = OneClassSVM(kernel='rbf', degree=3, gamma='scale', nu=nu)

    # Fit the model on the data
    oc_svm.fit(X_scaled)

    # Predict the anomalies
    y_pred = oc_svm.predict(X_scaled)

    # Add predictions to the DataFrame
    df['predicted_anomaly'] = y_pred

    # Visualizing in 3D
    fig = plt.figure(figsize=(12, 12)) 
    ax = fig.add_subplot(111, projection='3d')

    # Normal data points
    ax.scatter(df.loc[df['predicted_anomaly'] == 1, 'x'],
               df.loc[df['predicted_anomaly'] == 1, 'y'],
               df.loc[df['predicted_anomaly'] == 1, 'z'],
               c='blue', label='Normal')

    # Anomalous data points
    ax.scatter(df.loc[df['predicted_anomaly'] == -1, 'x'],
               df.loc[df['predicted_anomaly'] == -1, 'y'],
               df.loc[df['predicted_anomaly'] == -1, 'z'],
               c='red', label='Anomaly')

    ax.set_xlabel('East-West Movement (mm)')
    ax.set_ylabel('North-South Movement (mm)')
    ax.set_zlabel('Up-Down Movement (mm)')
    plt.title('One-Class SVM Anomaly Detection')
    plt.legend()
    plt.show()
