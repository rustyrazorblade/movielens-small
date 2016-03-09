from pandas import read_csv
from zipfile import ZipFile

movie_fields = [ "id", "name", "release_date", "video_release_date", "url", "unknown",
                         "Action", "Adventure", "Animation", "Children's", "Comedy", "Crime",
                         "Documentary", "Drama", "Fantasy", "Film-Noir", "Horror",  "Musical",
                         "Mystery", "Romance", "Sci-Fi", "Thriller", "War", "Western"]
def read_movies(zfp):
    fp = zfp.open("ml-100k/u.item")


    movies = read_csv(fp, sep="|", header=None, index_col=0,
                    names=movie_fields).fillna(0)

    movies['genres'] = movies.loc[:, 'unknown':'Western'].apply(lambda row: [row.index[row.astype('bool')]], axis=1)
    return movies


def read_users(zfp):
    fp = zfp.open("ml-100k/u.user")
    return read_csv(fp, sep="|", header=None,
             names=["id", "age", "gender", "occupation", "zip"], index_col=0)


def read_ratings(zfp):
    fp = zfp.open("ml-100k/u.data")
    names = ["user_id", "movie_id", "rating", "timestamp"]
    ratings = read_csv(fp, sep="\t", header=None, names=names)
    return ratings




def get_zip(context):
    fp = context.download("http://files.grouplens.org/datasets/movielens/ml-100k.zip")
    zf = ZipFile(file=fp)
    return zf
