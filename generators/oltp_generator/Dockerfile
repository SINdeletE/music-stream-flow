FROM python:3.14-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app

COPY generators/oltp_generator/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY generators/__init__.py /app/generators/__init__.py
COPY generators/catalogs/ /app/generators/catalogs/
COPY generators/oltp_generator/*.py /app/generators/oltp_generator/

USER 10001:10001
CMD ["python", "-m", "generators.oltp_generator"]
