drop table Twitter;

create table Twitter (
  followed int,
  follower int)
row format delimited fields terminated by ',' stored as textfile;

load data local inpath '${hiveconf:G}' overwrite into table Twitter;

select t2.f2, COUNT(1)
from (select t1.follower as f1, COUNT(t1.followed) as f2
      from Twitter as t1
      group by t1.follower) t2
group by t2.f2;
