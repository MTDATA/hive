CREATE DATABASE db_to_be_dropped_1;

CREATE DATABASE db_to_be_dropped_2;

CREATE DATABASE db_to_be_dropped_3;

CREATE DATABASE db_to_be_dropped_4;


grant Select on database db_to_be_dropped_2 to user hive_test_user;

grant Drop on database db_to_be_dropped_3 to user hive_test_user;

grant All on database db_to_be_dropped_4 to user hive_test_user;

show grant user hive_test_user on database db_to_be_dropped_1;

show grant user hive_test_user on database db_to_be_dropped_2;

show grant user hive_test_user on database db_to_be_dropped_3;

show grant user hive_test_user on database db_to_be_dropped_4;

SHOW DATABASES;

set hive.security.authorization.enabled=true;

DROP DATABASE db_to_be_dropped_3;

DROP DATABASE db_to_be_dropped_4;

SHOW DATABASES;

DROP DATABASE db_to_be_dropped_1;

DROP DATABASE db_to_be_dropped_2;

SHOW DATABASES;
