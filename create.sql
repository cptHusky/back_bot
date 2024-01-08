CREATE TABLE phrase_usage (
    id SERIAL PRIMARY KEY,
    times_used INT DEFAULT 0
);

INSERT INTO phrase_usage (id)
SELECT generate_series(0, 49);
