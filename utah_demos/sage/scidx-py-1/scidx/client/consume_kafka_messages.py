import requests
import json
import pandas as pd
import time
import threading
import IPython.display as display

class KafkaMessageConsumer:
    def __init__(self, url, params=None, headers=None, retry_interval=5, verbose=False):
        self.url = url
        self.params = params or {}
        self.headers = headers or {}
        self._retry_interval = retry_interval
        self._stop_event = threading.Event()
        self.data_list = []
        self._df = pd.DataFrame()  # Initialize an empty DataFrame to store the data
        self._started = False  # Track if data consumption has successfully started
        self.verbose = verbose  # Control printing based on verbose flag

        # Start the consumption thread
        self._thread = threading.Thread(target=self._consume_messages)
        self._thread.start()

    def _consume_messages(self):
        while not self._stop_event.is_set():
            try:
                with requests.get(self.url, params=self.params, headers=self.headers, stream=True, timeout=10) as response:
                    response.raise_for_status()

                    for line in response.iter_lines():
                        if self._stop_event.is_set():
                            break
                        if line:
                            data = json.loads(line.decode("utf-8"))

                            # Check for "error" message indicating topic not yet available
                            if "error" in data:
                                if self.verbose:
                                    print(f"\rWaiting for stream data to be ready for topic '{self.params.get('topic')}'... Retrying every {self._retry_interval} seconds.", end="")
                                time.sleep(self._retry_interval)
                                break  # Break the loop to retry
                            
                            # If data successfully received for the first time
                            if not self._started:
                                if self.verbose:
                                    print(f"\nConsuming started successfully for topic '{self.params.get('topic')}'.")
                                self._started = True  # Set started to True

                            # Append data to the list and update DataFrame
                            self.data_list.append(data)
                            self._df = self._append_data_to_df(data, self._df)

                        #time.sleep(0.25)

            except requests.RequestException as e:
                if self.verbose:
                    print(f"Error consuming stream: {e}. Retrying in {self._retry_interval} seconds...")
                time.sleep(self._retry_interval)

    def _append_data_to_df(self, data, df):
        """Normalize nested 'values' dictionary and dynamically add new columns to DataFrame."""
        new_df = pd.json_normalize(data['values'])
        return pd.concat([df, new_df], ignore_index=True).fillna("N/A")


    def summary(self, columns=None, interval=2):
        """
        Displays a real-time summary of the latest data row and general statistics.

        Parameters
        ----------
        columns : list of str, optional
            List of column names to display in the latest row view (default is None, showing all columns).
        interval : int, optional
            The time interval in seconds between updates (default is 2).
        """
        try:
            # Divide interval into smaller sleep chunks to check stop event more frequently
            sleep_chunk = 0.1
            iterations = int(interval / sleep_chunk)

            while not self._stop_event.is_set():
                # Clear and refresh the output
                display.clear_output(wait=True)
                
                # Filter the DataFrame columns to display, if specified
                if columns:
                    filtered_columns = [col for col in columns if col in self._df.columns]
                    latest_row = self._df[filtered_columns].tail(1)
                else:
                    latest_row = self._df.tail(1)
                
                # Display the latest row received with improved styling
                if not latest_row.empty:
                    display.display(latest_row.style.set_table_styles(
                        [
                            {'selector': 'th', 'props': [('font-weight', 'bold'), ('background-color', '#e8f4f8'), ('text-align', 'center')]},
                            {'selector': 'td', 'props': [('text-align', 'center'), ('padding', '6px'), ('background-color', '#f7fbfc')]}
                        ]
                    ).set_caption("Latest Data Row"))

                else:
                    print(f"No data received yet, retrying every {interval} seconds...")

                # Prepare general summary statistics as a DataFrame
                general_stats = pd.DataFrame({
                    'Total Rows': [len(self._df)],
                    'Total Columns': [len(self._df.columns)]
                })

                # Display general statistics table with styling
                display.display(general_stats.style.set_table_styles(
                    [
                        {'selector': 'th', 'props': [('font-weight', 'bold'), ('background-color', '#f2f2f2')]},
                        {'selector': 'td', 'props': [('text-align', 'center'), ('padding', '6px')]}
                    ]
                ).set_caption("Summary Statistics"))
                
                # Instead of one long sleep, do smaller sleeps to frequently check for stop
                for _ in range(iterations):
                    if self._stop_event.is_set():
                        return  # Exit if stop event is set
                    time.sleep(sleep_chunk)

        except KeyboardInterrupt:
            print("\nExiting summary display...")
                
        except KeyboardInterrupt:
            print("\nExiting summary display...")

    def stop(self):
        self._stop_event.set()
        self._thread.join()

    @property
    def dataframe(self):
        """Return the current DataFrame with messages."""
        return self._df


# Updated function to create and start the KafkaMessageConsumer with verbose control
def consume_kafka_messages(self, id=None, topic=None, host=None, port=None, timeout=30, verbose=False):
    """
    Returns a KafkaMessageConsumer instance that dynamically updates a DataFrame with stream data.

    Parameters
    ----------
    id : str, optional
        The ID of the Kafka stream.
    topic : str, optional
        The Kafka topic.
    host : str, optional
        The Kafka host.
    port : int, optional
        The Kafka port.
    timeout : int, optional
        The timeout for the request in seconds (default is 30).
    verbose : bool, optional
        If True, prints log messages (default is False).
    """
    url = f"{self.api_url}/stream"
    params = {"topic": topic} if topic else {}
    headers = self._get_headers()  # Adjust to fetch any necessary headers

    return KafkaMessageConsumer(url, params, headers, retry_interval=5, verbose=verbose)
