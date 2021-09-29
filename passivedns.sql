SELECT MIN(time), name, ttl, class, type, data FROM answers GROUP BY NAME, class, type, data ORDER BY time;

SELECT COUNT(DISTINCT name) FROM answers WHERE DATE(time) = '2021-05-19' AND TIME('now', 'localtime', '-1 hour') < TIME(time);

SELECT DISTINCT name FROM answers WHERE DATE(time) = '2021-05-18' AND TIME('now', 'localtime', '-1 hour') < TIME(time) ORDER BY time;
