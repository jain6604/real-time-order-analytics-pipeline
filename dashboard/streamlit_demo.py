"""Public Streamlit Community Cloud entrypoint for the explicitly static demo."""

import os

os.environ["APP_MODE"] = "demo"

import app  # noqa: F401,E402  - importing app renders the Streamlit page.
