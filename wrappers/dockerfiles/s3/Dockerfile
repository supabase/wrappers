# This docker file is s3-init image

FROM minio/mc:latest AS mc

FROM python:3.13-slim

COPY --from=mc /usr/bin/mc /mc

RUN pip install "pyiceberg[s3fs,pyarrow]==0.10.0" \
    && chmod +x /mc
