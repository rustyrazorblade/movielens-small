CREATE TABLE movies (
    id int primary key,
    name text,
    url text,
    genres set<text>
);

CREATE TABLE users (
    id int primary key,
    age int,
    gender text,
    occupation text,
    zip text
);

CREATE TABLE ratings_by_movie (
    movie_id int,
    user_id int,
    rating int,
    timestamp int,
    primary key(movie_id, user_id)
);

CREATE TABLE ratings_by_user (
    user_id int,
    movie_id int,
    name text,
    rating int,
    timestamp int,
    primary key (user_id, movie_id)
);
