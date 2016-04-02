import logging

from zipfile import ZipFile

import time
from pandas import read_csv

from cdm.installer import Installer, AutoGenerateSolrResources
from firehawk import parse_line
from movielens.models import *
from movielens.helpers import read_movies, read_users, get_zip, read_ratings


class MovieLensInstaller(Installer):
    def post_init(self):
        context = self.context

        zfp = get_zip(context)
        self.movies = read_movies(zfp.open("ml-100k/u.item"))
        self.users = read_users(zfp)
        self.ratings = read_ratings(zfp)

    def cassandra_schema(self):
        return [Movie, User, RatingsByMovie, RatingsByUser, OriginalMovieMap]

    def search_schema(self):
        return [AutoGenerateSolrResources(table="movies")]

    def install_cassandra(self):
        context = self.context
        logging.info("Installing movielens")

        for movie in self.movies.itertuples():
            # logging.info(row.name)
            Movie.create(id=movie.uuid, name=movie.name,
                         url=str(movie.url), genres=set(movie.genres[0]))

        logging.info("Movies done")

        for user in self.users.itertuples():
            User.create(id=user.uuid, age=user.age, gender=user.gender,
                        occupation=user.occupation, zip=user.zip,
                        name=user.name, address=user.address, city=user.city)

        logging.info("users done")

        # user id | item id | rating | timestamp
        prepared = context.session.prepare("INSERT INTO ratings_by_movie (movie_id, user_id, rating, ts) VALUES (?, ?, ?, ?)")
        prepared2 = context.session.prepare("INSERT INTO ratings_by_user (user_id, movie_id, name, rating, ts) VALUES (?, ?, ?, ?, ?)")

        i = 0
        start = time.time()

        for row in self.ratings.itertuples():
            movie_id = self.movies.ix[row.movie_id]["uuid"]
            user_id = self.users.ix[row.user_id]["uuid"]

            context.session.execute_async(prepared, (movie_id, user_id, row.rating, row.timestamp))
            movie_name = self.movies.ix[row.movie_id]["name"]
            future = context.session.execute_async(prepared2, (user_id, movie_id, movie_name, row.rating, row.timestamp))

            i += 1
            if i % 5000 == 0:
                future.result()
                rate = float(i) / (time.time() - start)
                logging.info("{} ratings processed, {} per sec".format(i, rate))


    def graph_schema(self):
        from firehawk import parse_line
        schema = ["CREATE VERTEX person",
                  "CREATE VERTEX movie",
                  "CREATE EDGE rated",
                  "CREATE PROPERTY id uuid",
                  "CREATE PROPERTY name text",
                  "CREATE PROPERTY rating int",
                  "CREATE MATERIALIZED INDEX user_id on vertex person(id)",
                  "CREATE MATERIALIZED INDEX movie_id on vertex movie(id)",
                  #"CREATE OUT INDEX rating_idx ON VERTEX person ON EDGE rated(rating)",
                    ]

        return [parse_line(s) for s in schema]


    def install_graph(self):
        # create movies
        session = self.context.session
        from dse.graph import SimpleGraphStatement

        movie_stmt = SimpleGraphStatement("graph.addVertex(label, 'movie', 'name', name, 'id', movie_id)")
        person_stmt = SimpleGraphStatement("graph.addVertex(label, 'person', 'age', age, 'gender', gender, 'id', user_id, 'name', name)")

        rate2 = """g.V().has('person', 'id', user_id).as('a').
                  V().has('movie', 'id', movie_id).as('b').
                  addE("rated").property('rating', rating).from("a").to("b")"""

        rate2_stmt = SimpleGraphStatement(rate2)

        for movie in self.movies.itertuples():
            try:
                params = {"name": movie.name,
                          "movie_id": str(movie.uuid)}

                session.execute_graph(movie_stmt, params)
            except Exception as e:
                print e, movie


        i = 0
        for user in self.users.itertuples():
            try:
                args = {"age":user.age,
                        "gender": user.gender,
                        "user_id": str(user.uuid),
                        "name": user.name}
                session.execute_graph(person_stmt, args)
            except Exception as e:
                logging.warn("%s %s", args, e)
            i += 1
            if i % 100 == 0:
                print i

        i = 0
        lim = 1000
        import time
        start = time.time()
        for rating in self.ratings.itertuples():

            try:
                user_id = str(self.users.ix[rating.user_id].uuid)
                movie_id = str(self.movies.ix[rating.movie_id].uuid)
                params = {"user_id": user_id,
                          "movie_id": movie_id,
                          "rating": rating.rating}

                session.execute_graph(rate2_stmt, params)
            except Exception as e:
                print params, e
            i += 1
            if i == lim:
                i = 0
                total = time.time() - start
                start = time.time()
                print float(lim) / total, "edges per second"

