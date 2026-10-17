FROM python:3.14-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app

COPY generators/event_generator/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY generators/__init__.py /app/generators/__init__.py
COPY generators/event_generator/*.py /app/generators/event_generator/
COPY generators/event_generator/src/ /app/generators/event_generator/src/
COPY generators/catalogs/ /app/generators/catalogs/

USER 10001:10001
CMD ["python", "-m", "generators.event_generator"]
