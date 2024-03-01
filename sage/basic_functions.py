import asyncio
import json
import os
import webbrowser
from datetime import datetime
from IPython.display import clear_output, display
import ipywidgets as widgets
from ipywidgets import IntText, HBox, Label
from aiokafka import AIOKafkaConsumer
from plotly.graph_objs import FigureWidget, Scatter
from plotly.subplots import make_subplots
import plotly.graph_objects as go
import numpy as np
import nest_asyncio
nest_asyncio.apply()
from ai_forecast import IncrementalModel, parse_timestamp
import requests
import pandas as pd


def get_and_display_consumer_data(api_url):
    # Making a GET request to the API
    response = requests.get(api_url + '/active_consumers')
    data = response.json()  # Parsing the JSON response
    
    # Creating a list to hold the consumer data and a list for consumer IDs
    consumers = []
    consumer_ids = []
    for consumer_id, details in data.items():
        interests = ", ".join(details["profile"]["interests"])
        name = details["profile"]["attributes"]["name"]
        vsn = details["profile"]["attributes"].get("vsn", "")
        sensor = details["profile"]["attributes"].get("sensor", "")
        
        # Append a dictionary for each consumer to the list
        consumers.append({"Consumer ID": consumer_id, "Interests": interests, "Name": name, "VSN": vsn, "Sensor": sensor})
        
        # Append the consumer ID to the consumer_ids list
        consumer_ids.append(consumer_id)
    
    # Creating a DataFrame from the list
    df = pd.DataFrame(consumers)
    
    # Displaying the DataFrame as a table
    display(df)
    
    # Return the array of consumer IDs
    return consumer_ids


def timestamp_to_formatted_str(unix_timestamp):
    return datetime.utcfromtimestamp(unix_timestamp).strftime('%m/%d/%Y-%H:%M:%S')


async def stream_and_visualize_data(kafka_host, kafka_port, consumer_id, predictions):
    consumer = AIOKafkaConsumer(
        f"consumer_{consumer_id}",
        bootstrap_servers=kafka_host + ":" + kafka_port,
        auto_offset_reset="latest"
    )
    timestamp=True
    save=False
    await consumer.start()
    model = IncrementalModel(max_data_points=100)

    # Setup for saving the figure
    filename = consumer_id + ".html"
    filepath = os.path.join(os.getcwd(), filename)

    # Initialize the FigureWidget with layout options
    fig = go.FigureWidget(
        layout=go.Layout(
            title="Real-Time Data and Predictions for consumer " + consumer_id,
            xaxis=dict(title="Timestamp", tickformat="%m/%d/%Y-%H:%M:%S"),
            yaxis=dict(title="Value"),
            autosize=True,
            template="plotly_dark",
            margin=dict(l=50, r=50, t=50, b=50)
        )
    )

    # Setup for saving the figure
    filename = consumer_id +".html"
    filepath = os.path.join(os.getcwd(), filename)
    
    fig.add_scatter(name='Real Data - Historical', mode='lines')
    fig.add_scatter(name='Real Data - Latest', mode='lines+markers')
    fig.add_scatter(name='Predictions - Historical', mode='lines', line=dict(dash='dot'))
    fig.add_scatter(name='Predictions - Latest', mode='lines+markers', line=dict(dash='dot'))

    display(fig)

    try:
        while True:
            async for msg in consumer:
                data = json.loads(msg.value)
                timestamp = parse_timestamp(data["data"]["timestamp"])
                value = data["data"]["value"]
                model.update_data([{'timestamp': timestamp, 'value': value}])
                model.train_and_predict(num_predictions=predictions)

                with fig.batch_update():
                    # Convert timestamps to formatted strings before updating the plot
                    if timestamp:
                        timestamps_formatted = [ts for ts in model.all_data['timestamp']]
                        predictions_timestamps_formatted = [ts for ts in model.all_predictions['timestamp']]
                    else:
                        timestamps_formatted = [timestamp_to_formatted_str(ts) for ts in model.all_data['timestamp']] 
                        predictions_timestamps_formatted = [timestamp_to_formatted_str(ts) for ts in model.all_predictions['timestamp']]
                    
                    # Update historical data
                    fig.data[0].x = timestamps_formatted[:-1]
                    fig.data[0].y = model.all_data['value'].iloc[:-1]
                    # Update latest real data point
                    fig.data[1].x = timestamps_formatted[-1:]
                    fig.data[1].y = model.all_data['value'].iloc[-1:]
                    # Update historical predictions
                    fig.data[2].x = predictions_timestamps_formatted[:-predictions]
                    fig.data[2].y = model.all_predictions['value'].iloc[:-predictions]
                    # Update latest predictions
                    fig.data[3].x = predictions_timestamps_formatted[-predictions:]
                    fig.data[3].y = model.all_predictions['value'].iloc[-predictions:]

                    # Adjust y-axis range dynamically based on data and predictions
                    all_values = np.concatenate([model.all_data['value'], model.all_predictions['value']])
                    y_range = [all_values.min() - 5, all_values.max() + 5]  # Add some padding
                    fig.layout.yaxis.range = y_range

                    # Dynamically update title
                    #total_data_points = len(model.all_data['value'])
                    #total_predictions = len(model.all_predictions['value'])
                    #fig.layout.title.text = f"Real-Time Data and Predictions - {total_data_points} Data Points, {total_predictions} Predictions"

                if save:
                    fig.write_html(filepath, auto_open=False)
                    webbrowser.open('file://' + filepath, new=2)

                await asyncio.sleep(0.1)  # Control the update rate
    finally:
        await consumer.stop()