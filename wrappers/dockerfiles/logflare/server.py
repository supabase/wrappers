#!/usr/bin/env python3

import http.server
import socketserver
import json

from typing import Union
from fastapi import APIRouter, FastAPI, HTTPException

router = APIRouter(prefix="/v1")
data = json.loads(open("data.json").read())

@router.get("/{endpoint:path}")
def endpoint(
    endpoint: str,
    id: str | None = None,
    date_field: str | None = None,
    ts_field: str | None = None,
    tstz_field: str | None = None,
):
    if id is None and date_field is None and ts_field is None and tstz_field is None:
        return data

    def matches(item: dict) -> bool:
        if id is not None and str(item.get("id")) != id:
            return False
        if date_field is not None and str(item.get("date_field")) != date_field:
            return False
        if ts_field is not None and str(item.get("ts_field")) != ts_field.replace(" ", "T"):
            return False
        if tstz_field is not None and str(item.get("tstz_field"))[:10] != tstz_field[:10]:
            return False
        return True

    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict) and matches(item)]

    if isinstance(data, dict):
        if isinstance(data.get("result"), list):
            return {
                **data,
                "result": [
                    item for item in data["result"]
                    if isinstance(item, dict) and matches(item)
                ],
            }

        if matches(data):
            return data

        return {}

    return data

app = FastAPI()
app.include_router(router)
