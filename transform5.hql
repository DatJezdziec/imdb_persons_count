DROP TABLE IF EXISTS persons_ext;
DROP TABLE IF EXISTS names_ext;
DROP TABLE IF EXISTS out_json;

CREATE EXTERNAL TABLE IF NOT EXISTS persons_ext(
    nconst STRING,
    category STRING,
    amount INT)
COMMENT 'Persons Count'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
location '/user/lukaszjazwiec97/output_mr3';

CREATE EXTERNAL TABLE IF NOT EXISTS names_ext(
    nconst STRING,
    primaryName STRING,
    birthYear INT,
    deathYear INT,
    primaryProfession STRING,
    knwnForTitles STRING)
COMMENT 'Names Count'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
location '/user/lukaszjazwiec97/project_files/input/datasource4';

add jar /usr/lib/hive-hcatalog/share/hcatalog/hive-hcatalog-core-2.3.7.jar;

CREATE EXTERNAL TABLE IF NOT EXISTS out_json(
    name STRING,
    category STRING,
    amount INT)
ROW FORMAT SERDE
'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/user/lukaszjazwiec97/output6';

WITH cte AS (
  SELECT n.primaryName, p.category, p.amount, ROW_NUMBER() OVER (PARTITION BY p.category ORDER BY p.amount DESC) AS rn FROM persons_ext p JOIN names_ext n on (p.nconst = n.nconst)
  )
INSERT OVERWRITE table out_json
(SELECT cte.primaryName, cte.category, cte.amount FROM cte WHERE rn<=3 ORDER BY cte.category);