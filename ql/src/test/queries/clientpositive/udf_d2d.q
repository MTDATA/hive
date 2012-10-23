DESCRIBE FUNCTION d2d;
DESCRIBE FUNCTION EXTENDED d2d;

select d2d('20120909', "", "-") from src limit 1;
select d2d(null, "", "-") from src limit 1;
select d2d('null', "", "-") from src limit 1;
