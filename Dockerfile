FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN python -c "import airbyte as ab; ab.get_source('source-github', config={'credentials': {'personal_access_token': 'dummy'}, 'repositories': ['dbt-labs/dbt-core'], 'start_date': '2024-01-01T00:00:00Z'})"

RUN cd dbt_github && dbt deps && dbt parse --profiles-dir /app/dbt_github --target dev_no_connection && find /app/dbt_github/target -name "manifest.json" && cd ..

ENV DAGSTER_HOME=/app/.dagster_home
RUN mkdir -p $DAGSTER_HOME && \
    echo "telemetry:\n  enabled: false" > /app/.dagster_home/dagster.yaml

EXPOSE 3000

CMD ["python", "-m", "orchestration.run"]