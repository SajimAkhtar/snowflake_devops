create or replace external table ET_PC_POLICYLOCATION(
	FILE_TIMESTAMP VARCHAR(16777216) AS (SUBSTR(METADATA$FILENAME, 23, 23)),
	SOURCE_FILENAME VARCHAR(16777216) AS (METADATA$FILENAME),
	PUBLICID VARCHAR(16777216) AS (CAST(GET($1, 'PublicID') AS VARCHAR(16777216))),
	CITYKANJIINTERNALDENORM VARCHAR(16777216) AS (CAST(GET($1, 'CityKanjiInternalDenorm') AS VARCHAR(16777216))),
	ADDRESSLINE1INTERNAL VARCHAR(16777216) AS (CAST(GET($1, 'AddressLine1Internal') AS VARCHAR(16777216))),
	COUNTYINTERNAL VARCHAR(16777216) AS (CAST(GET($1, 'CountyInternal') AS VARCHAR(16777216))),
	ADDRESSLINE2INTERNAL VARCHAR(16777216) AS (CAST(GET($1, 'AddressLine2Internal') AS VARCHAR(16777216))),
	ADDRESSLINE3INTERNAL VARCHAR(16777216) AS (CAST(GET($1, 'AddressLine3Internal') AS VARCHAR(16777216))),
	CREATETIME TIMESTAMP_NTZ(9) AS (CAST(GET($1, 'CreateTime') AS TIMESTAMP_NTZ(9))),
	CITYKANJIINTERNAL VARCHAR(16777216) AS (CAST(GET($1, 'CityKanjiInternal') AS VARCHAR(16777216))),
	ADDRESSLINE2KANJIINTERNAL VARCHAR(16777216) AS (CAST(GET($1, 'AddressLine2KanjiInternal') AS VARCHAR(16777216))),
	STATEINTERNAL NUMBER(38,0) AS (TO_NUMBER(GET($1, 'StateInternal'))),
	FIXEDID NUMBER(38,0) AS (TO_NUMBER(GET($1, 'FixedID'))),
	COUNTRYINTERNAL NUMBER(38,0) AS (TO_NUMBER(GET($1, 'CountryInternal'))),
	EFFECTIVEDATE TIMESTAMP_NTZ(9) AS (CAST(GET($1, 'EffectiveDate') AS TIMESTAMP_NTZ(9))),
	UPDATETIME TIMESTAMP_NTZ(9) AS (CAST(GET($1, 'UpdateTime') AS TIMESTAMP_NTZ(9))),
	ID NUMBER(38,0) AS (TO_NUMBER(GET($1, 'ID'))),
	EXPIRATIONDATE TIMESTAMP_NTZ(9) AS (CAST(GET($1, 'ExpirationDate') AS TIMESTAMP_NTZ(9))),
	EMPLOYEECOUNTINTERNAL NUMBER(38,0) AS (TO_NUMBER(GET($1, 'EmployeeCountInternal'))),
	VALIDUNTILINTERNAL TIMESTAMP_NTZ(9) AS (CAST(GET($1, 'ValidUntilInternal') AS TIMESTAMP_NTZ(9))),
	TAXLOCATION NUMBER(38,0) AS (TO_NUMBER(GET($1, 'TaxLocation'))),
	CREATEUSERID NUMBER(38,0) AS (TO_NUMBER(GET($1, 'CreateUserID'))),
	ACCOUNTLOCATION NUMBER(38,0) AS (TO_NUMBER(GET($1, 'AccountLocation'))),
	CITYINTERNALDENORM VARCHAR(16777216) AS (CAST(GET($1, 'CityInternalDenorm') AS VARCHAR(16777216))),
	INDUSTRYCODEID NUMBER(38,0) AS (TO_NUMBER(GET($1, 'IndustryCodeID'))),
	ARCHIVEPARTITION NUMBER(38,0) AS (TO_NUMBER(GET($1, 'ArchivePartition'))),
	BEANVERSION NUMBER(38,0) AS (TO_NUMBER(GET($1, 'BeanVersion'))),
	CITYINTERNAL VARCHAR(16777216) AS (CAST(GET($1, 'CityInternal') AS VARCHAR(16777216))),
	CHANGETYPE NUMBER(38,0) AS (TO_NUMBER(GET($1, 'ChangeType'))),
	ADDRESSTYPEINTERNAL NUMBER(38,0) AS (TO_NUMBER(GET($1, 'AddressTypeInternal'))),
	ADDRESSLINE1KANJIINTERNAL VARCHAR(16777216) AS (CAST(GET($1, 'AddressLine1KanjiInternal') AS VARCHAR(16777216))),
	CEDEXBUREAUINTERNAL VARCHAR(16777216) AS (CAST(GET($1, 'CEDEXBureauInternal') AS VARCHAR(16777216))),
	BASEDONID NUMBER(38,0) AS (TO_NUMBER(GET($1, 'BasedOnID'))),
	UPDATEUSERID NUMBER(38,0) AS (TO_NUMBER(GET($1, 'UpdateUserID'))),
	LOCATIONNUM NUMBER(38,0) AS (TO_NUMBER(GET($1, 'LocationNum'))),
	POSTALCODEINTERNALDENORM VARCHAR(16777216) AS (CAST(GET($1, 'PostalCodeInternalDenorm') AS VARCHAR(16777216))),
	CEDEXINTERNAL NUMBER(38,0) AS (TO_NUMBER(GET($1, 'CEDEXInternal'))),
	BUILDINGAUTONUMBERSEQ NUMBER(38,0) AS (TO_NUMBER(GET($1, 'BuildingAutoNumberSeq'))),
	POSTALCODEINTERNAL VARCHAR(16777216) AS (CAST(GET($1, 'PostalCodeInternal') AS VARCHAR(16777216))),
	DESCRIPTIONINTERNAL VARCHAR(16777216) AS (CAST(GET($1, 'DescriptionInternal') AS VARCHAR(16777216))),
	BRANCHID NUMBER(38,0) AS (TO_NUMBER(GET($1, 'BranchID'))),
	FIREPROTECTCLASS NUMBER(38,0) AS (TO_NUMBER(GET($1, 'FireProtectClass'))))
location=@CSVLOADING/
auto_refresh=false
pattern='.*dbo_PC_POLICYLOCATION_.*'
file_format=(TYPE=PARQUET NULL_IF=())
;