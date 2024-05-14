FROM python:3.10-slim

WORKDIR /usr/src/app

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl

COPY . .
RUN pip install -r requirements.txt

CMD [ "uvicorn", "server:app", "--host", "0.0.0.0", "--port", "4242" ]
