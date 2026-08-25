FROM python:3.13.2-slim

ENV PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

COPY server.py /app/server.py

USER 65534:65534

EXPOSE 8787

CMD ["python", "-B", "/app/server.py"]
