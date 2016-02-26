CREATE TABLE movies (
    id int primary key,
    name text,
    url text,
    genres set<text>
);
