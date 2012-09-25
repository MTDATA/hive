DESCRIBE FUNCTION substring_index;
DESCRIBE FUNCTION EXTENDED substring_index;

SELECT
  substring_index('www.mysql.com', '.', -1),
  substring_index('www.mysql.com', '.', 2)
FROM src LIMIT 1;
