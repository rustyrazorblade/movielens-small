import logging

from zipfile import ZipFile
from pandas import read_csv

from cdm.installer import Installer
from firehawk import parse_line
from movielens.models import Movie, User, RatingsByMovie, RatingsByUser
from movielens.helpers import read_movies, read_users, get_zip, read_ratings


class MovieLensInstaller(Installer):
    def post_init(self):
        context = self.context

        zfp = get_zip(context)
        self.movies = read_movies(zfp)
        self.users = read_users(zfp)
        self.ratings = read_ratings(zfp)

    def cassandra_schema(self):
        return [Movie, User, RatingsByMovie, RatingsByUser]


    def install_cassandra(self):
        context = self.context
        context.feedback("Installing movielens")

        for movie in self.movies.itertuples():
            # context.feedback(row.name)
            try:
                Movie.create(id=movie.Index, name=movie.name.encode("utf-8"),
                             url=str(movie.url), genres=set(movie.genres[0]))
            except Exception as e:
                print e, movie
        context.feedback("Movies done")
        for user in self.users.itertuples():
            try:
                User.create(id=user.Index, age=user.age, gender=user.gender,
                            occupation=user.occupation, zip=user.zip,
                            name=user.name, address=user.address, city=user.city)
            except Exception as e:
                print user.Index, e
        context.feedback("users done")

        # user id | item id | rating | timestamp
        prepared = context.session.prepare("INSERT INTO ratings_by_movie (movie_id, user_id, rating, ts) VALUES (?, ?, ?, ?)")
        prepared2 = context.session.prepare("INSERT INTO ratings_by_user (user_id, movie_id, name, rating, ts) VALUES (?, ?, ?, ?, ?)")

        i = 0
        for row in self.ratings.itertuples():
            context.session.execute_async(prepared, (row.movie_id, row.user_id, row.rating, row.timestamp))
            try:
                movie_name = self.movies.iloc[row.movie_id]["name"]
                future = context.session.execute_async(prepared2, (row.user_id, row.movie_id, movie_name, row.rating, row.timestamp))
            except IndexError:
                print "Could not find movie, probably an encoding issue from earlier, movie: ", row.movie_id

            i += 1
            if i % 2500 == 0:
                future.result()
                context.feedback("{} ratings processed".format(i))


    def graph_schema(self):
        from firehawk import parse_line
        schema = ["CREATE VERTEX person",
                  "CREATE VERTEX movie",
                  "CREATE EDGE rated",
                  "CREATE PROPERTY id int",
                  "CREATE PROPERTY name text",
                  "CREATE PROPERTY rating int",
                  "CREATE MATERIALIZED INDEX user_id on vertex person(id)",
                  "CREATE MATERIALIZED INDEX movie_id on vertex movie(id)",
                  "CREATE OUT INDEX rating_idx ON VERTEX person ON EDGE rated(rating)"]

        return [parse_line(s) for s in schema]


    def install_graph(self):
        # create movies
        session = self.context.session
        from dse.graph import SimpleGraphStatement

        movie_stmt = SimpleGraphStatement("graph.addVertex(label, 'movie', 'name', name, 'id', movie_id)")
        person_stmt = SimpleGraphStatement("graph.addVertex(label, 'person', 'age', age, 'gender', gender, 'id', user_id, 'name', name)")

        rate2 = """g.V().has('person', 'id', user_id).as('a').
                  V().has('movie', 'id',movie_id).as('b').
                  addE("rated").property('rating', rating).from("a").to("b")"""

        rate2_stmt = SimpleGraphStatement(rate2)

        for movie in self.movies.itertuples():
            try:
                params = {"name": movie.name,
                          "movie_id": movie.Index}

                session.execute_graph(movie_stmt, params)
            except Exception as e:
                print e, movie


        i = 0
        for user in self.users.itertuples():
            try:
                args = {"age":user.age,
                        "gender": user.gender,
                        "user_id": user.Index,
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

            params = {"user_id": rating.user_id,
                      "movie_id": rating.movie_id,
                      "rating": rating.rating}
            try:
                session.execute_graph(rate2_stmt, params)
            except Exception as e:
                print params, e
            i += 1
            if i == lim:
                i = 0
                total = time.time() - start
                start = time.time()
                print float(lim) / total, "edges per second"

