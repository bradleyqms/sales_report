# FastAPI Sales Report Web App

A simple web application built with FastAPI, Uvicorn, HTML, and CSS for generating sales reports.

## Setup

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Run the application:
   ```bash
   uvicorn main:app --reload
   ```

3. Open your browser to `http://localhost:8000`

## Features

- Generate sales reports by clicking "Run Report"
- View real-time output in the textarea
- Download individual files (CSV, TXT, PDF) or ZIP archive
- Clean, responsive UI

## Architecture

- `main.py`: FastAPI application with endpoints
- `templates/index.html`: HTML template
- `static/styles.css`: CSS styles
- Background task execution for report generation
- File serving for downloads