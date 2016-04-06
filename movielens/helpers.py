from pandas import read_csv
from zipfile import ZipFile
from faker import Factory
import uuid

movie_fields = [ "id", "name", "release_date", "video_release_date", "url", "unknown",
                         "Action", "Adventure", "Animation", "Children's", "Comedy", "Crime",
                         "Documentary", "Drama", "Fantasy", "Film-Noir", "Horror",  "Musical",
                         "Mystery", "Romance", "Sci-Fi", "Thriller", "War", "Western"]
def read_movies(fp):
    movies = read_csv(fp, sep="|", header=None, index_col=0, names=movie_fields, encoding="latin_1").fillna(0)

    movies['genres'] = movies.loc[:, 'unknown':'Western'].apply(lambda row: [row.index[row.astype('bool')]], axis=1)
    movies['uuid'] = movies.apply(lambda _: uuid.uuid4(), axis=1)
    return movies[["name", "release_date", "video_release_date", "url", "genres", "uuid"]]


def read_users(zfp):
    fp = zfp.open("ml-100k/u.user")
    users = read_csv(fp, sep="|", header=None,
             names=["id", "age", "gender", "occupation", "zip"], index_col=0)
    f = Factory.create()

    names = ["Jon Haddad",
             "Dani Traphagen",
             "Patrick McFadin",
             "Mark Quinsland",
             "Brian Hess",
             "Russell Spitzer",
             "Lorina Poland",
             "Tim Berglund",
             "Tupshin Harper",
             "Al Tobey"]

    names.reverse()

    def get_name(row):
        if names:
            return names.pop()
        return f.name()

    users['name'] = users.apply(get_name, axis=1)
    users['city'] = users.apply(lambda row: f.city(), axis=1)
    users['address'] = users.apply(lambda row: f.street_address(), axis=1)
    users['uuid'] = users.apply(lambda _: uuid.uuid4(), axis=1)

    return users


def read_ratings(zfp):
    fp = zfp.open("ml-100k/u.data")
    names = ["user_id", "movie_id", "rating", "timestamp"]
    ratings = read_csv(fp, sep="\t", header=None, names=names)
    return ratings




def get_zip(context):
    fp = context.download("http://files.grouplens.org/datasets/movielens/ml-100k.zip")
    zf = ZipFile(file=fp)
    return zf

