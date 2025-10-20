# --- Base image ---
    FROM python:3.10-slim

    # --- Working directory ---
    WORKDIR /app
    
    # --- Copy all project files ---
    COPY . /app
    
    # --- Install dependencies ---
    RUN pip install --no-cache-dir -r requirements.txt
    
    RUN mkdir -p data/raw data/processed
    
    RUN cd src
    # --- Default command: run full pipeline ---
    CMD ["python", "pipeline.py"]
    