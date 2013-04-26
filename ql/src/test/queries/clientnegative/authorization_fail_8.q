set hive.security.authorization.enabled=false;

create table src_authorization_8 (key1 int, value1 string);
set hive.security.authorization.enabled=true;

desc src_authorization_8;

drop table default.src_authorization_8;
