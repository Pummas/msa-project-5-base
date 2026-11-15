FROM python:3.12.7-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN addgroup --system exporter && adduser --system --ingroup exporter exporter
WORKDIR /app
COPY app/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app/export_events.py .
RUN mkdir /export && chown exporter:exporter /export
USER exporter
ENTRYPOINT ["python", "/app/export_events.py"]
