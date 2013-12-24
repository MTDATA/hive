create table default_alter6 (a int) partitioned by (p string);
alter table default_alter6 add partition (p='p0');
LOAD DATA LOCAL INPATH '../data/files/in1.txt' into table default_alter6 PARTITION (p='p0');
show create table default_alter6;
select * from default_alter6;

create database db6;
alter table default.default_alter6 rename to db6.alter6;
use db6;
show create table db6.alter6;
select * from db6.alter6;

alter table db6.alter6 add columns (b int);
describe db6.alter6;

alter table db6.alter6 replace columns (a int, b int, c string);
describe db6.alter6;

alter table db6.alter6 change column c cc int;
describe db6.alter6;

show partitions alter6;
alter table db6.alter6 add partition (p='p1');
show partitions alter6;

alter table db6.alter6 touch partition(p='p1');

set hive.archive.enabled = true;
alter table db6.alter6 archive partition (p='p1');
describe extended alter6 partition (p='p1');

alter table db6.alter6 unarchive partition (p='p1');
describe extended alter6 partition (p='p1');
show partitions alter6;

alter table db6.alter6 drop partition (p='p1');
show partitions alter6;

alter table db6.alter6 set tblproperties('s1'='1');
describe extended alter6;

alter table db6.alter6 unset tblproperties('s1'='1');
describe extended alter6;

alter table db6.alter6 partition (p='p0') set fileformat orc;
show create table alter6;

alter table db6.alter6 partition column (p int);
describe db6.alter6;

alter table db6.alter6 skewed by (a) on ('skew_a') stored as directories;
describe formatted db6.alter6;
alter table db6.alter6 not stored as directories;
describe formatted db6.alter6;
alter table db6.alter6 not skewed;
describe formatted db6.alter6;

drop table db6.alter6;
drop database db6;
