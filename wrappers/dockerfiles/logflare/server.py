#!/usr/bin/env python3

import http.server
import socketserver
import json

from typing import Union
from fastapi import APIRouter, FastAPI, HTTPException

router = APIRouter(prefix="/v1")
data = json.loads(open("data.json").read())

@router.get("/{endpoint:path}")
def endpoint():
    return data

app = FastAPI()
app.include_router(router)
