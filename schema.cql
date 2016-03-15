CREATE TABLE movies (
    id int primary key,
    name text,
    url text,
    genres set<text>,
    release_date date
);

CREATE TABLE users (
    id int primary key,
    name text,
    city text,
    address text,
    age int,
    gender text,
    occupation text,
    zip text
);

CREATE TABLE ratings_by_movie (
    movie_id int,
    user_id int,
    rating int,
    ts int,
    primary key(movie_id, user_id)
);

CREATE TABLE ratings_by_user (
    user_id int,
    movie_id int,
    name text,
    rating int,
    ts int,
    primary key (user_id, movie_id)
);

