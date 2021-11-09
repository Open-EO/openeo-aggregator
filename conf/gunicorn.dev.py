# See https://docs.gunicorn.org/en/stable/settings.html
from pathlib import Path

bind = ["0.0.0.0:8080"]

workers = 1
threads = 1

# Worker timeout
timeout = 15

logconfig = str(Path(__file__).parent / "logging-json.conf")

print(f"loaded {__file__}")
