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
