SELECT MIN(time), name, ttl, class, type, data FROM answers GROUP BY NAME, class, type, data ORDER BY time;

SELECT COUNT(DISTINCT name) FROM answers WHERE DATE(time) = '2021-05-19' AND TIME('now', 'localtime', '-1 hour') < TIME(time);

SELECT DISTINCT name FROM answers WHERE DATE(time) = '2021-05-18' AND TIME('now', 'localtime', '-1 hour') < TIME(time) ORDER BY time;

SELECT * FROM answers ORDER BY time DESC LIMIT 10;


SELECT MIN(time), name, ttl, class, type, data FROM answers GROUP BY name, ttl, class, type, data ORDER BY time, id;


UPDATE answers SET name = RTRIM(name, '.') WHERE name LIKE '%.';
UPDATE answers SET data = RTRIM(data, '.') WHERE type = "CNAME" AND data LIKE '%.';
