from cdm.testing import context
from pytest import fixture

from movielens.helpers import read_users, read_movies, get_zip

@fixture(scope="module")
def z(context):
    return get_zip(context)

@fixture
def movies(context):
    z = get_zip(context)
    return read_movies(z)


def test_movie_parsing(movies):
    pass


def test_user_parsing(context, z):
    assert True

def test_user_parsing2(context):
    assert True
