from zipfile import ZipFile
from pandas import read_csv

from cassandra.cqlengine.models import Model
from cassandra.cqlengine.columns import *
from cassandra.cqlengine.management import sync_table
from cassandra.cqlengine.connection import set_session


class Movie(Model):
    __table_name__ = 'movies'
    id = Integer(primary_key=True)
    name = Text()
    release_date = Date()
    video_release_date = Date()
    url = Text()
    tags = Set(Text)


def main(context):
    sync_table(Movie)
    context.feedback("Installing movielens")
    fp = context.download("http://files.grouplens.org/datasets/movielens/ml-100k.zip")
    zf = ZipFile(file=fp)
    tmp = zf.open("ml-100k/u.item")
    items = read_csv(tmp, sep="|", header=None, names=["id", "name", "release_date", "video_release_date", "url", "unknown",
                                                        "Action", "Adventure", "Animation", "Children's", "Comedy", "Crime",
                                                        "Documentary", "Drama", "Fantasy", "Film-Noir", "Horror",  "Musical",
                                                        "Mystery", "Romance", "Sci-Fi", "Thriller", "War", "Western"])
    for row in items.itertuples():
        try:
            Movie.create(id=row.id, name=row.name.encode("utf-8"),
                         url=str(row.url))
        except:
            print row

if __name__ == "__main__":
    from cdm import install_local
    install_local("movielens-small", main)
