from typing import List

from fastapi import UploadFile
from pydantic import BaseModel


class MarkupRequest(BaseModel):
    file: UploadFile


class TestRequest(BaseModel):
    inn: str


class SearchRequest(BaseModel):
    text: str
    strict: bool = False


class SearchInnRequest(BaseModel):
    inns: List[int]


class SearchTextRequest(BaseModel):
    text: str
