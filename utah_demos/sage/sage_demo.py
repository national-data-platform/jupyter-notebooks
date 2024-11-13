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

sensor_data = [
    {
        "resource_name": "sage_demo_bme280_temp",
        "resource_title": "BME280 Temperature",
        "owner_org": "test_org",
        "resource_url": "https://data.sagecontinuum.org/api/v0/stream?sensor=bme280&name=env.temperature",
        "file_type": "stream"
    },
    {
        "resource_name": "sage_demo_bme280_pres",
        "resource_title": "BME280 Pressure",
        "owner_org": "test_org",
        "resource_url": "https://data.sagecontinuum.org/api/v0/stream?sensor=bme280&name=env.pressure",
        "file_type": "stream"
    },
    {
        "resource_name": "sage_demo_bme280_humid",
        "resource_title": "BME280 Humidity",
        "owner_org": "test_org",
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
    "IF state = 'Montana' AND temperature > 40 THEN alert = 'High Temp Montana'",
    "IF state = 'Oregon' AND temperature > 30 THEN alert = 'High Temp Oregon'",
    # Pressure-based alert
    "IF pressure > 101000 THEN alert = 'High Pressure Alert'",
    # Adjust pressure for certain alert conditions
    "IF alert IN ['High Temp Montana', 'High Temp Oregon'] THEN pressure = pressure * 0.95"
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


async def plot_temp_alerts(processor):
    # Initialize FigureWidget for dynamic plotting
    fig = go.FigureWidget(
        layout=go.Layout(
            title="Real-Time Temperature Data by State",
            xaxis=dict(title="Timestamp", tickformat="%H:%M:%S"),
            yaxis=dict(title="Temperature (°C)"),
            autosize=True,
            template="plotly_dark",
            margin=dict(l=50, r=50, t=50, b=50)
        )
    )

    # Display the figure widget only once
    display(fig)

    # Initialize data structures
    state_traces = {}         # Dictionary to store temperature trace indices by state
    alert_messages = deque(maxlen=10)  # Store up to 10 alert messages
    previous_alert_messages = list(alert_messages)  # To track changes in alerts

    # Add a single alert marker trace (initially empty, shared across states)
    fig.add_scatter(
        x=[], y=[],  # Initially empty
        mode='markers',
        name="Alert",  # Single "Alert" entry in the legend
        marker=dict(color='red', symbol='circle', size=10),  # Distinct red markers for alerts
        hoverinfo="text",  # Display alert message on hover
        showlegend=True  # Show a single "Alert" legend item
    )
    
    # Index of the alert trace (always the last trace added)
    alert_trace_idx = len(fig.data) - 1

    try:
        while True:
            # Get updated data from the processor's aggregated data
            new_df = processor.get_aggregated_df()

            # Reset alert data for each loop iteration
            alert_x, alert_y, alert_texts = [], [], []  # To store alert positions and hover texts

            with fig.batch_update():
                for state, group in new_df.groupby('state'):
                    # Check if this state already has a trace; if not, create one
                    if state not in state_traces:
                        # Use HSL to create a unique color for each state, avoiding red
                        state_color = f"hsl({(len(state_traces) * 36 + 30) % 360}, 70%, 50%)"
                        fig.add_scatter(
                            x=[], y=[],
                            mode='lines+markers',
                            name=state,
                            line=dict(color=state_color),
                            marker=dict(symbol='circle', size=4)
                        )
                        state_traces[state] = len(fig.data) - 1  # Store index of the state trace

                    # Filter out valid temperature data for plotting
                    group = group.dropna(subset=['temperature'])
                    if not group.empty:
                        temp_idx = state_traces[state]
                        fig.data[temp_idx].x = group['timestamp']
                        fig.data[temp_idx].y = group['temperature']

                    # Process alerts for this state with valid temperature and non-null alert values
                    alerts = group.dropna(subset=['alert'])
                    alerts = alerts[alerts['alert'] != 'None']
                    
                    if not alerts.empty:
                        alert_x.extend(alerts['timestamp'])
                        alert_y.extend(alerts['temperature'])
                        alert_texts.extend(alerts['alert'])

                        # Add recent alerts to the alert_messages queue
                        for _, row in alerts.iterrows():
                            alert_time = row['timestamp'].strftime("%H:%M:%S")
                            alert_msg = f"At {alert_time}, {row['alert']} alert in {state}."
                            if alert_msg not in alert_messages:
                                alert_messages.append(alert_msg)

                # Update alert trace with consolidated alert data
                fig.data[alert_trace_idx].x = alert_x
                fig.data[alert_trace_idx].y = alert_y
                fig.data[alert_trace_idx].text = alert_texts  # Hover text with alert messages

                # Dynamically update y-axis range based on data
                if not new_df['temperature'].isna().all():
                    y_min = new_df['temperature'].min() - 5
                    y_max = new_df['temperature'].max() + 5
                    fig.layout.yaxis.range = [y_min, y_max]

            # Check if alert messages have changed before updating the display
            if list(alert_messages) != previous_alert_messages:
                previous_alert_messages = list(alert_messages)
                clear_output(wait=True)
                display(fig)
                print("Last 10 Alerts:")
                for message in alert_messages:
                    print(message)

            await asyncio.sleep(2)

    except asyncio.CancelledError:
        print("Visualization stopped.")




