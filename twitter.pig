E = LOAD '$G' USING PigStorage(',') AS (key1:long, value1:long);
G = GROUP E BY value1;
F = FOREACH G GENERATE group AS (key2:long), COUNT(E) AS (value2:long);
H = GROUP F BY value2;
P = FOREACH H GENERATE group AS (key3:long), COUNT(F) AS (value3:long);  
STORE P INTO '$O' USING PigStorage (' ');
