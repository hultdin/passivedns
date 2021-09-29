SELECT MIN(time), name, ttl, class, type, data FROM answers GROUP BY NAME, class, type, data ORDER BY time;

SELECT COUNT(DISTINCT name) FROM answers WHERE DATE(time) = '2021-05-19' AND TIME('now', 'localtime', '-1 hour') < TIME(time);

SELECT DISTINCT name FROM answers WHERE DATE(time) = '2021-05-18' AND TIME('now', 'localtime', '-1 hour') < TIME(time) ORDER BY time;

SELECT * FROM answers ORDER BY time DESC LIMIT 10;


SELECT MIN(time), name, ttl, class, type, data FROM answers GROUP BY name, ttl, class, type, data ORDER BY time, id;


UPDATE answers SET name = RTRIM(name, '.') WHERE name LIKE '%.';
UPDATE answers SET data = RTRIM(data, '.') WHERE type = "CNAME" AND data LIKE '%.';


SELECT * FROM answers a WHERE a.name = 'www.tm.a.prd.aadg.akadns.net' AND a.time != (SELECT MIN(time) FROM answers b WHERE a.name = b.name AND a.ttl = b.ttl AND a.class = b.class AND a.type = b.type AND a.data = b.data) GROUP BY name;

SELECT * FROM answers a WHERE a.name = 'www.swedbank.se' AND a.time NOT IN (SELECT MIN(time) FROM answers b WHERE a.name = b.name AND a.ttl = b.ttl AND a.class = b.class AND a.type = b.type AND a.data = b.data);


DELETE FROM answers a WHERE a.name = 'www.tm.a.prd.aadg.akadns.net' AND a.time != (SELECT MIN(time) FROM answers b WHERE a.name = b.name AND a.ttl = b.ttl AND a.class = b.class AND a.type = b.type AND a.data = b.data) GROUP BY name;

DELETE FROM answers a WHERE a.time NOT IN (SELECT MIN(time) FROM answers b WHERE a.name = b.name AND a.ttl = b.ttl AND a.class = b.class AND a.type = b.type AND a.data = b.data);


go build -ldflags "-s -w"



SELECT * FROM answers WHERE answers.name = 'www.swedbank.se' AND answers.time NOT IN (SELECT MIN(time) FROM answers a WHERE answers.name = a.name AND answers.ttl = a.ttl AND answers.class = a.class AND answers.type = a.type AND answers.data = a.data);
DELETE FROM answers WHERE answers.name = 'www.swedbank.se' AND answers.time NOT IN (SELECT MIN(time) FROM answers a WHERE answers.name = a.name AND answers.ttl = a.ttl AND answers.class = a.class AND answers.type = a.type AND answers.data = a.data);
