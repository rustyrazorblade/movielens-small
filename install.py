from zipfile import ZipFile
from pandas import read_csv

from cdm.installer import Installer
from movielens.models import Movie, User

class MovieLensInstaller(Installer):
    def post_init(self):
        context = self.context
        fp = context.download("http://files.grouplens.org/datasets/movielens/ml-100k.zip")
        self.zf = ZipFile(file=fp)
        tmp = self.zf.open("ml-100k/u.item")
        self.movies = read_csv(tmp, sep="|", header=None, index_col=0,
                         names=[ "id", "name", "release_date", "video_release_date", "url", "unknown",
                                 "Action", "Adventure", "Animation", "Children's", "Comedy", "Crime",
                                 "Documentary", "Drama", "Fantasy", "Film-Noir", "Horror",  "Musical",
                                 "Mystery", "Romance", "Sci-Fi", "Thriller", "War", "Western"])

        users = self.zf.open("ml-100k/u.user")
        self.users = read_csv(users, sep="|", header=None,
                         names=["id", "age", "gender", "occupation", "zip"], index_col=0)

        ratings = self.zf.open("ml-100k/u.data")
        names = ["user_id", "movie_id", "rating", "timestamp"]
        self.ratings = read_csv(ratings, sep="\t", header=None, names=names)


    def install_cassandra(self):
        context = self.context
        context.feedback("Installing movielens")

        for movie in self.movies.itertuples():
            # context.feedback(row.name)
            try:
                Movie.create(id=movie.Index, name=movie.name.encode("utf-8"),
                             url=str(movie.url))
            except Exception as e:
                print e, movie

        for user in self.users.itertuples():
            try:
                User.create(id=user.Index, age=user.age, gender=user.gender,
                            occupation=user.occupation, zip=user.zip)
            except Exception as e:
                print user.id, e

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
            if i % 1000 == 0:
                future.result()
                context.feedback("{} ratings processed".format(i))

    def install_graph(self):
        # create movies
        session = self.context.session
        from dse.graph import SimpleGraphStatement

        movie_stmt = SimpleGraphStatement("graph.addVertex(label, 'movie', 'name', n)")
        person_stmt = SimpleGraphStatement("graph.addVertex(label, 'person', 'age', age, 'gender', gender)")

        for movie in self.movies.itertuples():
            try:
                session.execute_graph(movie_stmt, {"n": movie.name})
            except Exception as e:
                print e, movie


        for user in self.users.itertuples():
            try:
                args = {"age":user.age, "gender": user.gender}
                session.execute_graph(person_stmt, args)
            except Exception as e:
                print args, e

