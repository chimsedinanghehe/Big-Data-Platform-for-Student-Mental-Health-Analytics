FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential librdkafka-dev \
    && useradd --create-home --uid 10001 appuser \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY --chown=appuser:appuser backend /app/backend
COPY --chown=appuser:appuser scripts/run_survey_snapshot_worker.py /app/scripts/run_survey_snapshot_worker.py
COPY --chown=appuser:appuser scripts/deployment/production_health_check.py /app/scripts/deployment/production_health_check.py
COPY --chown=appuser:appuser school_survey_questions_answers.txt university_survey_questions_answers.txt /app/

EXPOSE 8000

USER appuser

CMD ["uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "8000", "--proxy-headers", "--forwarded-allow-ips=*"]
