# scidx Streaming

A Python library for managing streaming data using the sciDX platform and a Point of Presence. This library provides easy-to-use methods for creating, consuming, and managing Kafka streams and related resources.


## Table of Contents

- [Installation](https://github.com/sci-ndp/streaming-py/blob/main/README.md#installation)
- [Tutorial](https://github.com/sci-ndp/streaming-py/blob/main/README.md#tutorial)
- [Running Tests](https://github.com/sci-ndp/streaming-py/blob/main/README.md#running-tests)
- [Configuration](https://github.com/sci-ndp/streaming-py/blob/main/README.md#configuration)
- [Contributing](https://github.com/sci-ndp/streaming-py/blob/main/README.md#contributing)
- [License](https://github.com/sci-ndp/streaming-py/blob/main/README.md#license)
- [Contact](https://github.com/sci-ndp/streaming-py/blob/main/README.md#contact)


## Installation

Ensure you have Python 3.7 or higher installed. Using a virtual environment is recommended.

### Option 1: Install from GitHub

1. **Clone the repository:**

   ```bash
   git clone https://github.com/sci-ndp/streaming-py.git
   cd streaming-py
   ```
2. **Create and activate a virtual environment:**

   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   ```
3. **Install the package in editable mode:**

   ```bash
   pip install -e .
   ```
4. **Install development dependencies (optional, for testing):**

   ```bash
   pip install -r requirements.txt
   ```

### Option 2: Install via pip

Once the package is published on PyPI, you can install it directly using pip:

```
pip install scidx_streaming
```

## Tutorial

For a step-by-step guide on how to use the `streaming` library, check out our comprehensive tutorial: [10 Minutes for Streaming POP Data](https://github.com/sci-ndp/streaming-py/blob/main/docs/streaming_tutorial.ipynb).


## Running Tests

To run the tests, navigate to the project root and execute:

```bash
pytest
```

## Configuration

To configure the library, you need to set the API URL for your POP API instance. This can be done by initializing the `APIClient` with the appropriate URL:

```python
from streaming import StreamingClient
from pointofpresence import APIClient

API_URL = "http://your-api-url.com"
USERNAME = "placeholder"
USERNAME = "placeholder"

client = APIClient(base_url=API_URL, username=USERNAME, password=PASSWORD)
streaming = StreamingClient(pop_client=client)
```

## Contributing

Contributions are welcome! Please follow these steps:

1. **Fork the repository**
2. **Create a new branch** (`git checkout -b feature/new-feature`)
3. **Make your changes** and **commit** (`git commit -m 'Add new feature'`)
4. **Push** to the branch (`git push origin feature/new-feature`)
5. **Open a Pull Reques**


## License

This project is licensed under the MIT License. See [LICENSE.md](https://github.com/sci-ndp/streaming-py/blob/main/docs/LICENSE.md) for more details.

## Contact

For any questions or suggestions, please open an [issue](https://github.com/sci-ndp/streaming-py/blob/main/docs/issues.md) on GitHub.
