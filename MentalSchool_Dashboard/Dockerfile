FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    STREAMLIT_BROWSER_GATHER_USAGE_STATS=false \
    STREAMLIT_SERVER_HEADLESS=true

WORKDIR /app

RUN useradd --create-home --uid 10001 appuser

COPY MentalSchool_Dashboard/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY --chown=appuser:appuser MentalSchool_Dashboard /app

EXPOSE 8501

USER appuser

CMD ["python", "-m", "streamlit", "run", "app.py", "--server.address=0.0.0.0", "--server.port=8501", "--server.headless=true", "--server.baseUrlPath=dashboard", "--server.enableCORS=false", "--server.enableXsrfProtection=false"]
