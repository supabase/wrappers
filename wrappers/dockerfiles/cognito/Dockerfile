FROM python:3.10-slim

WORKDIR /usr/src/app

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl

# Install AWS CLI and Boto3
RUN pip install --no-cache-dir awscli boto3

COPY ./server.py .

CMD [ "python", "./server.py" ]
