from cdm.testing import context
from pytest import fixture

from movielens.helpers import read_users, read_movies, get_zip, read_ratings


@fixture(scope="module")
def z(context):
    return get_zip(context)

@fixture
def movies(z):
    return read_movies(z)

@fixture
def users(z):
    return read_users(z)

@fixture
def ratings(z):
    return read_ratings(z)


def test_movie_parsing(movies):
    import ipdb; ipdb.set_trace()
    pass


def test_user_parsing(users):
    assert True


def test_user_parsing2(context):
    assert True
