# This docker file is s3-init image

FROM python:3.13-slim

RUN apt-get update && apt-get install -y --no-install-recommends wget
RUN pip install "pyiceberg[s3fs,pyarrow]==0.10.0" \
    && wget https://dl.min.io/client/mc/release/linux-amd64/mc \
    && chmod +x mc
