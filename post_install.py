from zipfile import ZipFile

def main(context):
    context.feedback("Installing movielens")
    fp = context.download("http://files.grouplens.org/datasets/movielens/ml-100k.zip")
    zf = ZipFile(file=fp)


if __name__ == "__main__":
    from cdm import install_local
    install_local("movielens-small", main)
