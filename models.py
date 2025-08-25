from typing import NotRequired, TypedDict

from bson import ObjectId
from pydantic import BaseModel, EmailStr


class EmailMessageDocument(TypedDict):
    _id: NotRequired[ObjectId]
    sender_email: str
    destination_email: str
    message: str


class SendEmailPartialDTO(BaseModel):
    sender_email: EmailStr
    message: str


class SendEmailDTO(SendEmailPartialDTO):
    destination_email: str
