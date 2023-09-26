create or replace external table ET_PC_HISTORY(
	FILE_TIMESTAMP VARCHAR(16777216) AS (SUBSTR(METADATA$FILENAME, 24, 23)),
	SOURCE_FILENAME VARCHAR(16777216) AS (METADATA$FILENAME),
	LOADCOMMANDID NUMBER(38,0) AS (TO_NUMBER(GET($1, 'LoadCommandID'))),
	ORIGINALVALUE VARCHAR(16777216) AS (CAST(GET($1, 'OriginalValue') AS VARCHAR(16777216))),
	PUBLICID VARCHAR(16777216) AS (CAST(GET($1, 'PublicID') AS VARCHAR(16777216))),
	USERID NUMBER(38,0) AS (TO_NUMBER(GET($1, 'UserID'))),
	ARCHIVEPARTITION NUMBER(38,0) AS (TO_NUMBER(GET($1, 'ArchivePartition'))),
	BEANVERSION NUMBER(38,0) AS (TO_NUMBER(GET($1, 'BeanVersion'))),
	ACCOUNTID NUMBER(38,0) AS (TO_NUMBER(GET($1, 'AccountID'))),
	CUSTOMTYPE NUMBER(38,0) AS (TO_NUMBER(GET($1, 'CustomType'))),
	POLICYID NUMBER(38,0) AS (TO_NUMBER(GET($1, 'PolicyID'))),
	NEWVALUE VARCHAR(16777216) AS (CAST(GET($1, 'NewValue') AS VARCHAR(16777216))),
	POLICYPERIOD NUMBER(38,0) AS (TO_NUMBER(GET($1, 'PolicyPeriod'))),
	RULEUID VARCHAR(16777216) AS (CAST(GET($1, 'RuleUID') AS VARCHAR(16777216))),
	POLICYTERMID NUMBER(38,0) AS (TO_NUMBER(GET($1, 'PolicyTermID'))),
	TYPE NUMBER(38,0) AS (TO_NUMBER(GET($1, 'LoadCommandID'))),
	SUBTYPE NUMBER(38,0) AS (TO_NUMBER(GET($1, 'Subtype'))),
	ID NUMBER(38,0) AS (TO_NUMBER(GET($1, 'ID'))),
	DESCRIPTION VARCHAR(16777216) AS (CAST(GET($1, 'Description') AS VARCHAR(16777216))),
	EVENTTIMESTAMP TIMESTAMP_NTZ(9) AS (CAST(GET($1, 'EventTimestamp') AS TIMESTAMP_NTZ(9))),
	JOB NUMBER(38,0) AS (TO_NUMBER(GET($1, 'Job'))))
location=@CSVLOADING/
auto_refresh=false
pattern='.*dbo_PC_HISTORY_.*'
file_format=(TYPE=PARQUET NULL_IF=())
;