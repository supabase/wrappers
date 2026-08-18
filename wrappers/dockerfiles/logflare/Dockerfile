FROM python:3.13.2-slim

WORKDIR /usr/src/app

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl

COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .

CMD [ "uvicorn", "server:app", "--host", "0.0.0.0", "--port", "4343" ]
