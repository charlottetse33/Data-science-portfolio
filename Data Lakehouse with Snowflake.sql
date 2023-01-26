--Data Ingestion

CREATE DATABASE bde_assignment_1;
USE DATABASE bde_assignment_1;
CREATE OR REPLACE STORAGE INTEGRATION azure_bde_assignment_1
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = AZURE
  ENABLED = TRUE
  AZURE_TENANT_ID = 'e8911c26-cf9f-4a9c-878e-527807be8791'
  STORAGE_ALLOWED_LOCATIONS = ('azure://utsbdeckltse.blob.core.windows.net/bde-assignment-1');
DESC STORAGE INTEGRATION azure_bde_assignment_1;
CREATE OR REPLACE STAGE stage_bde_assignment_1
STORAGE_INTEGRATION = azure_bde_assignment_1
URL = 'azure://utsbdeckltse.blob.core.windows.net/bde-assignment-1';
list @stage_bde_assignment_1;

--Ingest dataset in storage account on Azure

CREATE OR REPLACE FILE FORMAT file_format_csv 
TYPE = 'CSV' 
FIELD_DELIMITER = ',' 
SKIP_HEADER = 1
NULL_IF = ('\\N', 'NULL', 'NUL', '')
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
;
	--youtube trending ext(csv)
CREATE OR REPLACE EXTERNAL TABLE ex_table_youtube_trending
WITH LOCATION = @stage_bde_assignment_1
FILE_FORMAT = file_format_csv
PATTERN = '.*_youtube_trending_data.csv';

	--youtube category ext(json)
CREATE OR REPLACE EXTERNAL TABLE ex_table_youtube_category
WITH LOCATION = @stage_bde_assignment_1
FILE_FORMAT = (TYPE=json)
PATTERN = '.*_category_id.json';

--Transfer data from external table into tables
	--Trending data
CREATE OR REPLACE TABLE table_youtube_trending as 
(SELECT 
value:c1::varchar as VIDEO_ID, 
value:c2::varchar as TITLE,
value:c3::date as PUBLISHEDAT, 
value:c4::varchar as CHANNELID,
value:c5::varchar as CHANNELTITLE,
value:c6::int as CATEGORYID,
value:c7::date as TRENDING_DATE,
value:c8::int as VIEW_COUNT,
value:c9::int as LIKES,
value:c10::int as DISLIKES,
value:c11::int as COMMENT_COUNT,
value:c12::boolean as COMMENT_DISABLED, 
split_part(metadata$filename, '_', 1)::varchar as COUNTRY
from ex_table_youtube_trending);

	--Category data
CREATE OR REPLACE TABLE table_youtube_category as (
    SELECT 
    split_part(metadata$filename, '_', 1)::varchar as COUNTRY,
    l2.value:id::int as CATEGORYID, 
    l3.value:title::VARCHAR as CATEGORY_TITLE
    FROM ex_table_youtube_category
    , LATERAL FLATTEN(value) l1
    , LATERAL FLATTEN(l1.value) l2
    , LATERAL FLATTEN(l2.value) l3
    WHERE l1.key = 'items' and CATEGORY_TITLE IS NOT NULL
    ORDER BY COUNTRY, CATEGORYID
)
	--check duplicates
select count(*), category_title, categoryid, country from table_youtube_category group by category_title, categoryid, country having count(*) >1

--Create final table by combining above tables 
CREATE OR REPLACE TABLE table_youtube_final as (
    SELECT UUID_STRING() as ID, t.*, c.CATEGORY_TITLE
    FROM table_youtube_trending t
        LEFT JOIN 
    table_youtube_category c
    ON t.country = c.country 
    AND t.CATEGORYID = c.CATEGORYID
)
	--check for duplicates
SELECT COUNT(*), id FROM table_youtube_final group by id having count(*) >1

	--check for missing records 
SELECT count(*) from table_youtube_trending;
SELECT count(*) from table_youtube_final;

--Data cleaning
	--check for duplicates excluding categoryid
SELECT count(*), category_title, country 
FROM table_youtube_category 
GROUP BY category_title, country 
HAVING count(*)>1;
	--category_title only appears in one country
SELECT count(*), category_title 
FROM table_youtube_category 
GROUP BY category_title 
HAVING count(*) = 1;
	--categoryid of the missing category_title
SELECT distinct categoryid, category_title 
FROM table_youtube_final 
WHERE category_title is NULL;
	--Update table_youtube_final to replace the NULL values in category_title
CREATE OR REPLACE TABLE table_youtube_final as (
    SELECT 
    ID, 
    VIDEO_ID, 
    TITLE,
    PUBLISHEDAT,
    CHANNELID,
    CHANNELTITLE,
    CATEGORYID, 
    COALESCE(CATEGORY_TITLE, CATEGORYID::VARCHAR) AS CATEGORY_TITLE, 
    TRENDING_DATE,
    VIEW_COUNT,
    LIKES,
    DISLIKES,
    COMMENT_COUNT,
    COMMENT_DISABLED,
    COUNTRY
    FROM TABLE_YOUTUBE_FINAL
)
	--Video without channeltitle
SELECT * 
FROM table_youtube_final 
WHERE channeltitle IS NULL;
	--Delete record with video_id = “#NAME?”
CREATE OR REPLACE TABLE table_youtube_final as (
    SELECT * from table_youtube_final WHERE video_id != '#NAME?'
)
	--Create new table containing duplicates
CREATE OR REPLACE TABLE table_youtube_duplicates as(
    WITH cte AS(
        SELECT * ,ROW_NUMBER() OVER(PARTITION BY video_id, trending_date, country ORDER BY view_count DESC) AS dup
        FROM table_youtube_final
    )
    SELECT
		ID, 
		VIDEO_ID, 
		TITLE,
		PUBLISHEDAT,
		CHANNELID,
		CHANNELTITLE,
		CATEGORYID, 
		CATEGORY_TITLE, 
		TRENDING_DATE,
		VIEW_COUNT,
		LIKES,
		DISLIKES,
		COMMENT_COUNT,
		COMMENT_DISABLED,
		COUNTRY 
	FROM cte 
	WHERE dup >1
	--Delete duplicates via above table
CREATE OR REPLACE TABLE table_youtube_final as (
SELECT * from table_youtube_final 
    MINUS
SELECT *
FROM table_youtube_duplicates)
	--Check counts
SELECT count(*) FROM table_youtube_final;

--Data analysis
	--3 most viewed videos for each country in the “Sports” category for the trending_date = '2021-10-17'
WITH cte AS(
    SELECT *, RANK()OVER(PARTITON BY country ORDER BY view_count DESC) as RK 
    FROM table_youtube_final 
    WHERE category_title = 'Sports' AND trending_date = '2021-10-17'
)
SELECT * FROM cte where RK <4 order by country ASC, view_count DESC;
	--For each country, count the number of distinct video with a title containing the word “BTS” and order the result by count in a descending order
SELECT COUNT(DISTINCT video_id) AS ct, country 
FROM table_youtube_final 
WHERE CONTAINS(title,'BTS') 
GROUP BY country 
ORDER BY ct DESC;
	--For each country, year and month (in a single column), which video is the most viewed and what is its likes_ratio (defined as the percentage of likes against view_count)
SELECT 
    f.country,
    k.year_month,
    title,
    CHANNELTITLE,
    category_title,
    f.view_count, 
    CAST((likes/view_count)*100 AS DECIMAL(4,2)) AS LIKES_RATIO
FROM table_youtube_final f
    INNER JOIN 
    (SELECT country, 
            to_varchar(trending_date, 'YYYY-MM-01')as YEAR_MONTH,
            max(view_count) as max_view_count
     FROM table_youtube_final 
     GROUP BY country, YEAR_MONTH
    )k
ON f.country = k.country
AND to_varchar(f.trending_date, 'YYYY-MM-01') = k.year_month
AND f.view_count = k.max_view_count
ORDER BY year_month, country ASC
	--For each country, which category_title has the most distinct videos and what is its percentage (2 decimals) out of the total distinct number of videos of that country
WITH category_max AS(
    SELECT 
		category_title,
    RANK() OVER (ORDER BY total_cat DESC) rk 
    from (SELECT category_title, COUNT(distinct video_id) AS total_cat FROM table_youtube_final group by category_title)
),
category AS(
    SELECT 
		country, 
		category_title, 
		COUNT(DISTINCT video_id) AS total_category_video
    FROM table_youtube_final
    WHERE category_title = (SELECT category_title FROM category_max WHERE RK = 1)
    GROUP BY country, category_title
),
country AS(
    SELECT 
		country, 
		COUNT(DISTINCT video_id) AS total_country_video 
    FROM table_youtube_final
    GROUP BY country
)
SELECT 
	a.country, 
	a.category_title,
	a.total_category_video, 
	b.total_country_video, 
	CAST((a.total_category_video/b.total_country_video)*100 AS DECIMAL(4,2)) AS percentage
FROM category a
    LEFT JOIN
country b
ON a.country = b.country 
ORDER BY category_title, country;
	--channeltitle which produced the most distinct videos and what is this number
SELECT channeltitle,total_distinct_video FROM (
    SELECT 
    channeltitle, 
    total AS total_distinct_video,
    RANK() OVER (ORDER BY total DESC) rk 
    FROM (SELECT channeltitle , COUNT(DISTINCT video_id) AS total FROM table_youtube_final GROUP BY channeltitle)
)    
WHERE rk = 1

	--Check category title count
SELECT category_title, cnt, RANK() OVER (ORDER BY cnt DESC) AS rk 
FROM (SELECT category_title, count(*) AS cnt FROM table_youtube_final group BY category_title)
WHERE category_title NOT IN('Music', 'Entertainment');

	--Check category title count with % of distinct video
SELECT category_title, cnt, RANK() OVER (ORDER BY cnt DESC) AS rk 
FROM (SELECT category_title, COUNT(DISTINCT video_id) AS cnt FROM table_youtube_final GROUP BY category_title)
    WHERE category_title NOT IN('Music', 'Entertainment');

	--distinct channel/distinct video_id in category
SELECT a.category_title, a.category_cnt, b.channel_cnt, b.channel_cnt/a.category_cnt * 100 as perc, RANK() OVER (ORDER BY category_cnt DESC)as rk 
FROM (SELECT category_title, COUNT(DISTINCT video_id) AS category_cnt FROM table_youtube_final GROUP BY category_title) a
    INNER JOIN 
(SELECT COUNT(DISTINCT channeltitle) AS channel_cnt, category_title FROM table_youtube_final GROUP BY category_title) b
ON a.category_title = b.category_title 
WHERE a.category_title NOT IN('Music', 'Entertainment'); 

--d. include country
SELECT * 
FROM (SELECT a.country, a.category_title, a.category_cnt, b.channel_cnt, b.channel_cnt/a.category_cnt * 100 AS perc, RANK() OVER (PARTITION BY a.country ORDER BY category_cnt DESC)AS rk 
FROM (SELECT category_title, COUNT(DISTINCT video_id) AS category_cnt, country FROM table_youtube_final GROUP BY country, category_title) a
    INNER JOIN 
(SELECT COUNT(DISTINCT channeltitle) AS channel_cnt, category_title, country FROM table_youtube_final GROUP BY country, category_title) b
ON a.category_title = b.category_title 
AND a.country = b.country
WHERE a.category_title NOT IN('Music', 'Entertainment'))
WHERE rk <4
ORDER BY COUNTRY, RK