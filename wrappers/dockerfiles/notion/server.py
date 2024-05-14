#!/usr/bin/env python3

import http.server
import socketserver
import json

from typing import Union
from fastapi import APIRouter, FastAPI, HTTPException

router = APIRouter(prefix="/v1")
data = json.loads(open("data.json").read())


@router.get("/users")
def users():
    return {
        "object": "list",
        "next_cursor": None,
        "has_more": False,
        "type": "user",
        "request_id": "829ea7f8-97b4-4a05-a1f3-df6ef3c467b4",
        "results": data.get("users"),
    }


@router.get("/users/{user_id}")
def user(user_id: str):
    user = next((user for user in data.get("users") if user.get("id") == user_id), None)
    if user:
        return {
            "object": "user",
            "request_id": "829ea7f8-97b4-4a05-a1f3-df6ef3c467b4",
            **user,
        }

    raise HTTPException(404, "User not found")

app = FastAPI()
app.include_router(router)
