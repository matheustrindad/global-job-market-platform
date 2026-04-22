FROM python:3.10-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    streamlit==1.35.0 \
    plotly==5.22.0 \
    pandas==2.2.2 \
    requests==2.31.0

COPY app.py .

CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0", "--server.headless=true"]