from mongoengine import Document
from mongoengine.fields import StringField, BooleanField

class Messages(Document):
    fullname = StringField()
    email = StringField()
    processed = BooleanField(default=False)
    content = StringField()