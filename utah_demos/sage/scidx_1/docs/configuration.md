# Configuration

The `scidx` library requires configuration to connect to the `scidx-api`. You need to provide the base URL of the `scidx-api` when initializing the `sciDXClient`.

## Example Configuration

To configure the `sciDXClient`:

```python
from scidx.client import sciDXClient

api_url = "http://127.0.0.1:8000"
client = sciDXClient(api_url)
```

Replace `"http://127.0.0.1:8000"` with the actual URL where your `scidx-api` is running.

Return to [README.md](../README.md)
