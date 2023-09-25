create or replace external table ET_PC_ACCOUNTCONTACTROLE(
	RELATIONSHIPTITLE NUMBER(38,0) AS (TO_NUMBER(GET($1, 'RelationshipTitle'))),
	YEARLICENSED NUMBER(38,0) AS (TO_NUMBER(GET($1, 'YearLicensed'))),
	DATECOMPLETEDTRAININGCLASS TIMESTAMP_NTZ(9) AS (CAST(GET($1, 'DateCompletedTrainingClass') AS TIMESTAMP_NTZ(8))),
	CREATEUSERID NUMBER(38,0) AS (TO_NUMBER(GET($1, 'CreateUserID'), 8, 0)),
	NUMBEROFACCIDENTS NUMBER(38,0) AS (TO_NUMBER(GET($1, 'NumberofAccidents'))),
	PUBLICID VARCHAR(16777216) AS (CAST(GET($1, 'PublicID') AS VARCHAR(64))),
	INDUSTRYCODEID NUMBER(38,0) AS (TO_NUMBER(GET($1, 'IndustryCodeID'), 8, 0)),
	GOODDRIVERDISCOUNT BOOLEAN AS (CAST(GET($1, 'GoodDriverDiscount') AS BOOLEAN)),
	BEANVERSION NUMBER(38,0) AS (TO_NUMBER(GET($1, 'BeanVersion'))),
	RETIRED NUMBER(38,0) AS (TO_NUMBER(GET($1, 'Retired'), 8, 0)),
	CREATETIME TIMESTAMP_NTZ(9) AS (CAST(GET($1, 'CreateTime') AS TIMESTAMP_NTZ(8))),
	BUSOPSDESCRIPTION VARCHAR(16777216) AS (CAST(GET($1, 'BusOpsDescription') AS VARCHAR(60))),
	NUMBEROFVIOLATIONS NUMBER(38,0) AS (TO_NUMBER(GET($1, 'NumberofViolations'))),
	UPDATEUSERID NUMBER(38,0) AS (TO_NUMBER(GET($1, 'UpdateUserID'), 8, 0)),
	ACCOUNTCONTACT NUMBER(38,0) AS (TO_NUMBER(GET($1, 'AccountContact'), 8, 0)),
	UPDATETIME TIMESTAMP_NTZ(9) AS (CAST(GET($1, 'UpdateTime') AS TIMESTAMP_NTZ(8))),
	TRAININGCLASSTYPE NUMBER(38,0) AS (TO_NUMBER(GET($1, 'TrainingClassType'))),
	REFERENCED BOOLEAN AS (CAST(GET($1, 'Referenced') AS BOOLEAN)),
	SUBTYPE NUMBER(38,0) AS (TO_NUMBER(GET($1, 'Subtype'))),
	STUDENT BOOLEAN AS (CAST(GET($1, 'Student') AS BOOLEAN)),
	ID NUMBER(38,0) AS (TO_NUMBER(GET($1, 'ID'), 8, 0)),
	ORGTYPE VARCHAR(16777216) AS (CAST(GET($1, 'OrgType') AS VARCHAR(60))))
location=@CSVLOADING/
auto_refresh=false
pattern='.*dbo_PC_ACCOUNTCONTACTROLE_.*'
file_format=(TYPE=PARQUET NULL_IF=())
;