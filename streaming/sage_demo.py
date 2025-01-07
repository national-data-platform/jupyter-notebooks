import asyncio
import pandas as pd
import plotly.graph_objs as go
from datetime import datetime
from IPython.display import display, clear_output
from collections import deque

import numpy as np
import threading
import time
import warnings

import os
import asyncio
from dotenv import load_dotenv


# Load environment variables from .env file
load_dotenv(dotenv_path="/home/jovyan/work/streaming_library/.env")

API_URL = os.getenv("API_URL")
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")
EARTHSCOPE_USERNAME = os.getenv("EARTHSCOPE_USERNAME")
EARTHSCOPE_PASSWORD = os.getenv("EARTHSCOPE_PASSWORD")



sensor_data = [
    {
        "resource_name": "sage_demo_bme280_temp",
        "resource_title": "BME280 Temperature",
        "owner_org": "test_organization",
        "resource_url": "https://data.sagecontinuum.org/api/v0/stream?sensor=bme280&name=env.temperature",
        "file_type": "stream"
    },
    {
        "resource_name": "sage_demo_bme280_pres",
        "resource_title": "BME280 Pressure",
        "owner_org": "test_organization",
        "resource_url": "https://data.sagecontinuum.org/api/v0/stream?sensor=bme280&name=env.pressure",
        "file_type": "stream"
    },
    {
        "resource_name": "sage_demo_bme280_humid",
        "resource_title": "BME280 Humidity",
        "owner_org": "test_organization",
        "resource_url": "https://data.sagecontinuum.org/api/v0/stream?sensor=bme280&name=env.relative_humidity",
        "file_type": "stream"
    }
]

filters = [
    # Trim data to nodes for selected states
    "meta.vsn IN ['W070', 'W06F', 'W068', 'W085', 'W08C', 'W020']",
    # Map sensor types to corresponding data fields
    "IF name = env.temperature THEN temperature = value",
    "IF name = env.pressure THEN pressure = value",
    "IF name = env.relative_humidity THEN humidity = value",
    # Map sensors to states
    "IF meta.vsn = 'W070' THEN state = 'California'",
    "IF meta.vsn = 'W06F' THEN state = 'Montana'",
    "IF meta.vsn = 'W068' THEN state = 'Oregon'",
    "IF meta.vsn = 'W085' THEN state = 'North Dakota'",
    "IF meta.vsn = 'W08C' THEN state = 'Michigan'",
    "IF meta.vsn = 'W020' THEN state = 'Illinois'",
    # Heatwave alert
    "IF temperature > 35 OR humidity < 25 THEN alert = 'Heatwave' ELSE alert = 'None'",
    # State-based temperature thresholds for alerts
    # "IF state = 'Montana' AND temperature > 40 THEN alert = 'High Temp Montana'",
    # "IF state = 'Oregon' AND temperature > 30 THEN alert = 'High Temp Oregon'",
    # Pressure-based alert
    # "IF pressure > 101000 THEN alert = 'High Pressure Alert'",
    # Adjust pressure for certain alert conditions
    # "IF alert IN ['High Temp Montana', 'High Temp Oregon'] THEN pressure = pressure * 0.95"
]



class SageDataProcessing:
    def __init__(self, consumer, poll_interval=1):
        # Initialize with the consumer and create an empty processed dataframe
        self.consumer = consumer
        self.processed_df = pd.DataFrame(columns=['timestamp', 'temperature', 'state', 'alert', 'pressure', 'humidity'])
        self.last_processed_index = 0  # Keeps track of the last row processed in the consumer dataframe
        self.poll_interval = poll_interval  # Time interval for checking new data in seconds
        self.running = True  # Control flag for the background thread

        # Start background thread to continuously process new data
        self.processing_thread = threading.Thread(target=self._continuous_processing)
        self.processing_thread.start()

    def _continuous_processing(self):
        """Continuously process new data as it becomes available."""
        while self.running:
            self.process_new_data()
            time.sleep(self.poll_interval)  # Wait for the next polling interval

    def expand_row(self, row):
        """Expands a single row into multiple rows based on the number of entries in the arrays."""
        expanded_rows = []
        entry_count = len(row['timestamp'])  # Determine the number of entries
    
        for i in range(entry_count):
            try:
                pressure_value = (
                    pd.to_numeric(row['pressure'][i], errors='coerce')
                    if 'pressure' in row and i < len(row['pressure'])
                    else np.nan
                )
                new_row = {
                    'timestamp': row['timestamp'][i] if 'timestamp' in row and i < len(row['timestamp']) else np.nan,
                    'temperature': row['temperature'][i] if 'temperature' in row and i < len(row['temperature']) else np.nan,
                    'state': row['state'][i] if 'state' in row and i < len(row['state']) else np.nan,
                    'alert': row['alert'][i] if 'alert' in row and i < len(row['alert']) else np.nan,
                    'pressure': pressure_value,
                    'humidity': row['humidity'][i] if 'humidity' in row and i < len(row['humidity']) else np.nan
                }
                expanded_rows.append(new_row)
            except IndexError:
                # Log or handle any array length mismatches here if needed
                pass
    
        return expanded_rows


    def process_new_data(self):
        """Process new rows in the consumer dataframe and add to the processed dataframe."""
        df = self.consumer.dataframe
        new_rows = []
    
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=FutureWarning)
    
            for index in range(self.last_processed_index, len(df)):
                row = df.iloc[index]
                expanded_rows = self.expand_row(row)
                new_rows.extend(expanded_rows)
    
            if new_rows:
                new_rows_df = pd.DataFrame(new_rows)
                new_rows_df = new_rows_df.replace({"N": np.nan, "/": np.nan, "A": np.nan})
                new_rows_df.infer_objects(copy=False)
    
                if not new_rows_df.empty:
                    new_rows_df.dropna(axis=1, how='all', inplace=True)
                    # Sort by timestamp and state to ensure correct sequence in plots
                    new_rows_df.sort_values(by=['state', 'timestamp'], inplace=True)
                    self.processed_df = pd.concat([self.processed_df, new_rows_df], ignore_index=True)
                    
                self.last_processed_index = len(df)


    def aggregate_processed_data(self):
        """Aggregate the processed dataframe into 2-second intervals, grouped by time and state, prioritizing non-'None' alerts."""
        
        # Convert timestamp to datetime and floor to the nearest 5-second interval
        self.processed_df['timestamp'] = pd.to_datetime(self.processed_df['timestamp'])
        self.processed_df['timestamp'] = self.processed_df['timestamp'].dt.floor('2s')
        
        # Ensure 'pressure' is numeric before aggregation
        self.processed_df['pressure'] = pd.to_numeric(self.processed_df['pressure'], errors='coerce')
        
        # Filter out rows where all values are NaN for merging purposes
        main_df = self.processed_df.dropna(how='all', subset=['temperature', 'humidity', 'pressure'])
    
        # Step 1: Prioritize non-'None' alerts for aggregation
        main_df['alert_priority'] = main_df['alert'].apply(lambda x: 0 if x == 'None' else 1)
    
        # Step 2: Sort data to prioritize non-'None' alerts, and perform aggregation
        main_df = main_df.sort_values(by=['timestamp', 'state', 'alert_priority'], ascending=[True, True, False])
        aggregated_df = (
            main_df.groupby(['timestamp', 'state'], as_index=False)
            .agg({
                'alert': 'first',
                'temperature': 'mean',
                'humidity': 'mean',
                'pressure': 'mean'
            })
        )
    
        # Remove the 'alert_priority' helper column
        aggregated_df.drop(columns=['alert_priority'], errors='ignore', inplace=True)
        
        return aggregated_df



    def get_processed_df(self):
        """Returns the current processed dataframe, which is already updated."""
        return self.processed_df

    def get_aggregated_df(self):
        """Returns the aggregated dataframe."""
        return self.aggregate_processed_data()

    def stop(self):
        """Stops the continuous processing thread."""
        self.running = False
        self.processing_thread.join()  # Ensure the thread stops cleanly


import plotly.graph_objects as go
from collections import deque
import asyncio
from IPython.display import Image, display, clear_output
import pandas as pd

async def plot_temp_alerts(processor):
    # Initialize the figure
    fig = go.Figure(
        layout=go.Layout(
            title="Real-Time Temperature Data by State",
            xaxis=dict(title="Timestamp", tickformat="%H:%M:%S"),
            yaxis=dict(title="Temperature (°C)"),
            autosize=True,
            template="plotly_dark",
            margin=dict(l=50, r=50, t=50, b=50)
        )
    )

    state_traces = {}
    alert_messages = deque(maxlen=10)  # Stores only the latest 10 alerts

    # Separate DataFrame to persist all alerts and accumulated data
    alert_data = pd.DataFrame(columns=["timestamp", "temperature", "alert", "state"])
    accumulated_data = pd.DataFrame(columns=["timestamp", "temperature", "state", "alert"])

    # Initial display setup for alerts
    fig.add_scatter(
        x=[], y=[], mode='markers', name="Alert",
        marker=dict(color='red', symbol='circle', size=10), hoverinfo="text", showlegend=True
    )
    alert_trace_idx = len(fig.data) - 1

    try:
        while True:
            # Fetch new data and accumulate
            new_df = processor.get_aggregated_df()
            accumulated_data = pd.concat([accumulated_data, new_df]).drop_duplicates()

            # Append any new alerts to the persistent alert data
            new_alerts = new_df.dropna(subset=['alert'])
            new_alerts = new_alerts[new_alerts['alert'] != 'None']
            alert_data = pd.concat([alert_data, new_alerts]).drop_duplicates()

            # Prepare lists for temperature data and alerts
            alert_x, alert_y, alert_texts = alert_data['timestamp'], alert_data['temperature'], alert_data['alert']

            for state, group in accumulated_data.groupby('state'):
                if state not in state_traces:
                    state_color = f"hsl({(len(state_traces) * 36 + 30) % 360}, 70%, 50%)"
                    fig.add_scatter(
                        x=[], y=[], mode='lines+markers', name=state,
                        line=dict(color=state_color), marker=dict(symbol='circle', size=4)
                    )
                    state_traces[state] = len(fig.data) - 1

                temp_idx = state_traces[state]
                fig.data[temp_idx].x = group['timestamp']
                fig.data[temp_idx].y = group['temperature']

                # Add new alert messages to the alert_messages queue
                for _, row in new_alerts.iterrows():
                    alert_time = row['timestamp'].strftime("%H:%M:%S")
                    alert_msg = f"At {alert_time}, {row['alert']} alert in {state}."
                    alert_messages.append(alert_msg)

            # Update alert trace with all accumulated alerts
            fig.data[alert_trace_idx].x = alert_x
            fig.data[alert_trace_idx].y = alert_y
            fig.data[alert_trace_idx].text = alert_texts

            # Adjust y-axis range based on accumulated data
            if not accumulated_data['temperature'].isna().all():
                y_min = accumulated_data['temperature'].min() - 5
                y_max = accumulated_data['temperature'].max() + 5
                fig.layout.yaxis.range = [y_min, y_max]

            # Save and display the plot image
            fig.write_image("/tmp/plot.png", engine="kaleido", engine_config={"tmp_path": "/tmp"})
            clear_output(wait=True)
            display(Image("/tmp/plot.png"))

            # Print the latest 10 alerts consistently
            print("Last 10 Alerts:")
            for message in alert_messages:
                print(message)

            await asyncio.sleep(2)

    except asyncio.CancelledError:
        print("Visualization stopped.")

