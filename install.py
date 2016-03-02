from zipfile import ZipFile
from pandas import read_csv

from cassandra.cqlengine.models import Model
from cassandra.cqlengine.columns import *

from cdm.installer import Installer


class MovieLensInstaller(Installer):
    def install_cassandra(self):
        context = self.context
        context.feedback("Installing movielens")
        fp = context.download("http://files.grouplens.org/datasets/movielens/ml-100k.zip")
        zf = ZipFile(file=fp)
        tmp = zf.open("ml-100k/u.item")
        items = read_csv(tmp, sep="|", header=None, index_col=0,
                         names=[ "id", "name", "release_date", "video_release_date", "url", "unknown",
                                 "Action", "Adventure", "Animation", "Children's", "Comedy", "Crime",
                                 "Documentary", "Drama", "Fantasy", "Film-Noir", "Horror",  "Musical",
                                 "Mystery", "Romance", "Sci-Fi", "Thriller", "War", "Western"])
        for row in items.itertuples():
            # context.feedback(row.name)
            try:
                Movie.create(id=row.Index, name=row.name.encode("utf-8"),
                             url=str(row.url))
            except Exception as e:
                print e, row

        users = zf.open("ml-100k/u.user")
        users = read_csv(users, sep="|", header=None,
                         names=["id", "age", "gender", "occupation", "zip"], index_col=0)

        for user in users.itertuples():
            try:
                User.create(id=user.Index, age=user.age, gender=user.gender,
                            occupation=user.occupation, zip=user.zip)
            except Exception as e:
                print user.id, e

        # user id | item id | rating | timestamp
        data = zf.open("ml-100k/u.data")
        prepared = context.session.prepare("INSERT INTO ratings_by_movie (movie_id, user_id, rating, ts) VALUES (?, ?, ?, ?)")
        prepared2 = context.session.prepare("INSERT INTO ratings_by_user (user_id, movie_id, name, rating, ts) VALUES (?, ?, ?, ?, ?)")

        names = ["user_id", "movie_id", "rating", "timestamp"]
        ratings = read_csv(data, sep="\t", header=None, names=names)

        i = 0
        for row in ratings.itertuples():
            context.session.execute_async(prepared, (row.movie_id, row.user_id, row.rating, row.timestamp))
            try:
                movie_name = items.iloc[row.movie_id]["name"]
                context.session.execute_async(prepared2, (row.user_id, row.movie_id, movie_name, row.rating, row.timestamp))
            except IndexError:
                print "Could not find movie, probably an encoding issue from earlier, movie: ", row.movie_id

            i += 1
            if i % 1000 == 0:
                context.feedback("{} items processed".format(i))

        context.feedback("Done installing movielens-small")



