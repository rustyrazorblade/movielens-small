from cassandra.cqlengine.models import Model
from cassandra.cqlengine.columns import *

class Movie(Model):
    __table_name__ = 'movies'
    id = Integer(primary_key=True)
    name = Text()
    release_date = Date()
    video_release_date = Date()
    url = Text()
    avg_rating = Float()
    genres = Set(Text)


class User(Model):
    __table_name__ = "users"
    id = Integer(primary_key=True)
    age = Integer()
    gender = Text()
    occupation = Text()
    zip = Text()
    name = Text()
    city = Text()
    address = Text()
    

class RatingsByMovie(Model):
    movie_id = Integer(primary_key=True)
    user_id = Integer(primary_key=True)
    rating = Integer()
    ts = Integer()


class RatingsByUser(Model):
    user_id = Integer(primary_key=True)
    movie_id = Integer(primary_key=True)
    name = Text()
    rating = Integer()
    ts = Integer()

# CREATE TABLE ratings_by_movie (
#         movie_id int,
#                  user_id int,
#                          rating int,
#                                 ts int,
#                                    primary key(movie_id, user_id)
# );
#
# CREATE TABLE ratings_by_user (
#         user_id int,
#                 movie_id int,
#                          name text,
#                               rating int,
#                                      ts int,
#                                         primary key (user_id, movie_id)
# );

