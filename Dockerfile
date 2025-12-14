FROM python:3.11-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends libpq5 && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY . .

CMD sh -c "python -m migrations.create_tables && \
           python -m infrastructure.load_data.load_data && \
           python main.py"

#CMD sh -c "python main.py"