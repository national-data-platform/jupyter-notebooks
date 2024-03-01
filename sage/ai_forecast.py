import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from datetime import datetime
import asyncio
from aiokafka import AIOKafkaConsumer
import json
from plotly.graph_objs import FigureWidget, Scatter


# Helper function to parse timestamps
def parse_timestamp(timestamp_str):
    parts = timestamp_str.split(".")
    date_part = parts[0]
    microseconds = parts[1][:6] if len(parts) > 1 else '000000'
    timestamp_str = f"{date_part}.{microseconds}Z"
    return datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S.%fZ").timestamp()


class IncrementalModel:
    def __init__(self, max_data_points=100):
        self.all_data = pd.DataFrame(columns=['timestamp', 'value'])  # To store all data points
        self.all_predictions = pd.DataFrame(columns=['timestamp', 'value'])  # To store all predictions
        self.model = LinearRegression()
        self.max_data_points = max_data_points
        self.avg_timestamp_diff = None

    def update_data(self, new_data):
        new_df = pd.DataFrame(new_data)
        self.all_data = pd.concat([self.all_data, new_df], ignore_index=True)

        # Calculate avg_timestamp_diff using the most recent data points
        if len(self.all_data) > 1:
            self.avg_timestamp_diff = self.all_data['timestamp'].diff().iloc[-self.max_data_points:].mean()

    def train_and_predict(self, num_predictions=5):
        if self.avg_timestamp_diff is None or len(self.all_data) < 2:
            #print("Insufficient data to train and predict.")
            return

        # Use only the most recent max_data_points for training
        X_train = self.all_data[['timestamp']][-self.max_data_points:]
        y_train = self.all_data['value'][-self.max_data_points:]
        self.model.fit(X_train, y_train)

        last_timestamp = self.all_data.iloc[-1]['timestamp']
        future_timestamps = np.arange(1, num_predictions + 1) * self.avg_timestamp_diff + last_timestamp
        
        # Ensure the prediction input matches the training data structure
        future_timestamps_df = pd.DataFrame(future_timestamps.reshape(-1, 1), columns=['timestamp'])
        future_values = self.model.predict(future_timestamps_df)

        if not self.all_predictions.empty:
            # Keep predictions that are older than the last timestamp we're making predictions from
            self.all_predictions = self.all_predictions[self.all_predictions['timestamp'] < last_timestamp]
    
        # Append new predictions
        new_predictions = pd.DataFrame({'timestamp': future_timestamps, 'value': future_values})
        # Check if new_predictions DataFrame is not empty and does not contain all-NA columns
        if not new_predictions.dropna(how='all', axis=1).empty:
            self.all_predictions = pd.concat([self.all_predictions, new_predictions], ignore_index=True)        