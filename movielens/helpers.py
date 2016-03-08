from pandas import read_csv

def read_movies(fp):
    return read_csv(fp, sep="|", header=None, index_col=0,
                    names=[ "id", "name", "release_date", "video_release_date", "url", "unknown",
                            "Action", "Adventure", "Animation", "Children's", "Comedy", "Crime",
                            "Documentary", "Drama", "Fantasy", "Film-Noir", "Horror",  "Musical",
                            "Mystery", "Romance", "Sci-Fi", "Thriller", "War", "Western"])
