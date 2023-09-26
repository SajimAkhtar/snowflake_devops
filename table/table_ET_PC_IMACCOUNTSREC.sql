create or replace external table ET_PC_IMACCOUNTSREC(
	FILE_TIMESTAMP VARCHAR(16777216) AS (SUBSTR(METADATA$FILENAME, 22, 23)),
	SOURCE_FILENAME VARCHAR(16777216) AS (METADATA$FILENAME),
	INITIALCOVERAGESCREATED BOOLEAN AS (CAST(GET($1, 'InitialCoveragesCreated') AS BOOLEAN)),
	PUBLICID VARCHAR(16777216) AS (CAST(GET($1, 'PublicID') AS VARCHAR(16777216))),
	CREATETIME TIMESTAMP_NTZ(9) AS (CAST(GET($1, 'CreateTime') AS TIMESTAMP_NTZ(9))),
	FIXEDID NUMBER(38,0) AS (TO_NUMBER(GET($1, 'FixedID'))),
	EFFECTIVEDATE TIMESTAMP_NTZ(9) AS (CAST(GET($1, 'EffectiveDate') AS TIMESTAMP_NTZ(9))),
	UPDATETIME TIMESTAMP_NTZ(9) AS (CAST(GET($1, 'UpdateTime') AS TIMESTAMP_NTZ(9))),
	IMBUILDING NUMBER(38,0) AS (TO_NUMBER(GET($1, 'IMBuilding'))),
	ID NUMBER(38,0) AS (TO_NUMBER(GET($1, 'ID'))),
	FORWARDTOHOMEOFFICE BOOLEAN AS (CAST(GET($1, 'ForwardToHomeOffice') AS BOOLEAN)),
	EXPIRATIONDATE TIMESTAMP_NTZ(9) AS (CAST(GET($1, 'ExpirationDate') AS TIMESTAMP_NTZ(9))),
	INITIALEXCLUSIONSCREATED BOOLEAN AS (CAST(GET($1, 'InitialExclusionsCreated') AS BOOLEAN)),
	CREATEUSERID NUMBER(38,0) AS (TO_NUMBER(GET($1, 'CreateUserID'))),
	ARCHIVEPARTITION NUMBER(38,0) AS (TO_NUMBER(GET($1, 'ArchivePartition'))),
	BEANVERSION NUMBER(38,0) AS (TO_NUMBER(GET($1, 'BeanVersion'))),
	CHANGETYPE NUMBER(38,0) AS (TO_NUMBER(GET($1, 'ChangeType'))),
	INITIALCONDITIONSCREATED BOOLEAN AS (CAST(GET($1, 'InitialConditionsCreated') AS BOOLEAN)),
	BASEDONID NUMBER(38,0) AS (TO_NUMBER(GET($1, 'BasedOnID'))),
	RECEPTACLETYPE NUMBER(38,0) AS (TO_NUMBER(GET($1, 'ReceptacleType'))),
	UPDATEUSERID NUMBER(38,0) AS (TO_NUMBER(GET($1, 'UpdateUserID'))),
	ACCOUNTSRECNUMBER NUMBER(38,0) AS (TO_NUMBER(GET($1, 'AccountsRecNumber'))),
	REFERENCEDATEINTERNAL TIMESTAMP_NTZ(9) AS (CAST(GET($1, 'ReferenceDateInternal') AS TIMESTAMP_NTZ(9))),
	IMACCOUNTSRECPART NUMBER(38,0) AS (TO_NUMBER(GET($1, 'IMAccountsRecPart'))),
	SUBTYPE NUMBER(38,0) AS (TO_NUMBER(GET($1, 'Subtype'))),
	PREFERREDCOVERAGECURRENCY NUMBER(38,0) AS (TO_NUMBER(GET($1, 'PreferredCoverageCurrency'))),
	DESCRIPTION VARCHAR(100) AS (CAST(GET($1, 'Description') AS VARCHAR(16777216))),
	BRANCHID NUMBER(38,0) AS (TO_NUMBER(GET($1, 'BranchID'))),
	PERCENTDUPLICATED NUMBER(38,0) AS (TO_NUMBER(GET($1, 'PercentDuplicated'))))
location=@CSVLOADING/
auto_refresh=false
pattern='.*dbo_PC_IMACCOUNTSREC_.*'
file_format=(TYPE=PARQUET NULL_IF=())
;