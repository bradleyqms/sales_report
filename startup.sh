#!/bin/bash
cd fastapi_web_app
python -m uvicorn main:app --host 0.0.0.0 --port 8000