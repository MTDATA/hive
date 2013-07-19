CREATE TABLE t2 (letter string, name string, rank int, day int) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n';
load data local inpath '../data/files/t2.sql' overwrite into table t2 ;

select * from t2;
select day, group_concat(name, ",", `rank`) from t2 group by day;
