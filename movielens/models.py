from cassandra.cqlengine.models import Model
from cassandra.cqlengine.columns import *

class Movie(Model):
    __table_name__ = 'movies'
    id = Integer(primary_key=True)
    name = Text()
    release_date = Date()
    video_release_date = Date()
    url = Text()
    tags = Set(Text)

class User(Model):
    __table_name__ = "users"
    id = Integer(primary_key=True)
    age = Integer()
    gender = Text()
    occupation = Text()
    zip = Text()
