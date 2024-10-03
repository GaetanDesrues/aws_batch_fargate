FROM python:3.9

RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libeccodes-dev \
    python3-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY ./src /app/src
WORKDIR /app/src

CMD ["python", "main.py"]