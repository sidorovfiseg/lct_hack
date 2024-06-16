from fastapi import UploadFile
from pydantic import BaseModel

class MarkupRequest(BaseModel):
    file: UploadFile

class TestRequest(BaseModel):
    inn: str