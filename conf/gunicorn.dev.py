# See https://docs.gunicorn.org/en/stable/settings.html

bind = ["0.0.0.0:8080"]

workers = 1
threads = 1

# Worker timeout
timeout = 60

print(f"loaded {__file__}")
