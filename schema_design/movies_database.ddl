CREATE SCHEMA content


CREATE TABLE IF NOT EXISTS content.film_work (
    id uuid PRIMARY KEY,
    title TEXT NOT NULL,
    description TEXT,
    creation_date DATE,
    rating FLOAT,
    type TEXT NOT NULL,
    created TIMESTAMP WITH TIME ZONE,
    modified TIMESTAMP WITH TIME ZONE
);

CREATE UNIQUE INDEX filmwork_idx ON content.film_work (id, title, creation_date, type);


CREATE TABLE IF NOT EXISTS content.genre (
    id uuid PRIMARY KEY,
    name TEXT,
    description TEXT,
    created TIMESTAMP WITH TIME ZONE,
    modified TIMESTAMP WITH TIME ZONE
);

CREATE UNIQUE INDEX genre_idx ON content.genre (id, name);


CREATE TABLE IF NOT EXISTS content.person (
    id uuid PRIMARY KEY,
    full_name TEXT,
    created TIMESTAMP WITH TIME ZONE,
    modified TIMESTAMP WITH TIME ZONE
);


CREATE TABLE IF NOT EXISTS content.genre_film_work (
    id uuid PRIMARY KEY,
    genre_id uuid NOT NULL REFERENCES content.genre (Id) ON DELETE CASCADE,
    film_work_id uuid NOT NULL REFERENCES content.film_work (Id) ON DELETE CASCADE,
    created TIMESTAMP WITH TIME ZONE
);



CREATE TABLE IF NOT EXISTS content.person_film_work (
    id uuid PRIMARY KEY,
    person_id uuid NOT NULL REFERENCES content.person (Id) ON DELETE CASCADE,
    film_work_id uuid NOT NULL REFERENCES content.film_work (Id) ON DELETE CASCADE,
    role TEXT,
    created TIMESTAMP WITH TIME ZONE
);


   
ALTER ROLE current_user set search_path TO content, public;