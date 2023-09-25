create or replace external table ET_PCTL_BOPCOST(
	L_EN_US VARCHAR(16777216) AS (CAST(GET($1, 'L_en_US') AS VARCHAR(16777216))),
	PRIORITY NUMBER(38,0) AS (TO_NUMBER(GET($1, 'PRIORITY'))),
	TYPECODE VARCHAR(16777216) AS (CAST(GET($1, 'TYPECODE') AS VARCHAR(16777216))),
	S_EN_US NUMBER(38,0) AS (TO_NUMBER(GET($1, 'S_en_US'))),
	RETIRED BOOLEAN AS (CAST(GET($1, 'RETIRED') AS BOOLEAN)),
	NAME VARCHAR(16777216) AS (CAST(GET($1, 'NAME') AS VARCHAR(16777216))),
	ID NUMBER(38,0) AS (TO_NUMBER(GET($1, 'ID'))),
	DESCRIPTION VARCHAR(16777216) AS (CAST(GET($1, 'DESCRIPTION') AS VARCHAR(16777216))))
location=@CSVLOADING/
auto_refresh=false
pattern='.*dbo_PCTL_BOPCOST_.*'
file_format=(TYPE=PARQUET NULL_IF=())
;