"""Public Streamlit Community Cloud entrypoint for the explicitly static demo."""

import os

os.environ["APP_MODE"] = "demo"

import app

app.main()
