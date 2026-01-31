FROM python:3.11-slim

WORKDIR /pipeline
COPY requirements.txt /pipeline/
COPY pipeline/ /pipeline/
RUN pip install --no-cache-dir -r requirements.txt


CMD ["python", "/pipeline/src/main.py"]
