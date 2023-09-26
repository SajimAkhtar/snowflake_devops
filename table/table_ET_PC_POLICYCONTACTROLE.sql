create or replace external table ET_PC_POLICYCONTACTROLE(
	LICENSENUMBERINTERNAL VARCHAR(20) AS (CAST(GET($1, 'LicenseNumberInternal') AS VARCHAR(20))),
	EXCLUDEDINTERNAL BOOLEAN AS (CAST(GET($1, 'ExcludedInternal') AS BOOLEAN)),
	COMPANYNAMEKANJIINTERNALDENORM VARCHAR(255) AS (CAST(GET($1, 'CompanyNameKanjiInternalDenorm') AS VARCHAR(255))),
	FIXEDID NUMBER(8,0) AS (TO_NUMBER(GET($1, 'FixedID'), 8, 0)),
	STATE NUMBER(38,0) AS (TO_NUMBER(GET($1, 'State'))),
	OWNERSHIPPCT NUMBER(38,0) AS (TO_NUMBER(GET($1, 'OwnershipPct'))),
	COMMERCIALPROPERTYLINE NUMBER(8,0) AS (TO_NUMBER(GET($1, 'CommercialPropertyLine'), 8, 0)),
	UPDATETIME TIMESTAMP_NTZ(9) AS (CAST(GET($1, 'UpdateTime') AS TIMESTAMP_NTZ(9))),
	DATEOFBIRTHINTERNAL TIMESTAMP_NTZ(9) AS (CAST(GET($1, 'DateOfBirthInternal') AS TIMESTAMP_NTZ(9))),
	GENERALLIABILITYLINE NUMBER(8,0) AS (TO_NUMBER(GET($1, 'GeneralLiabilityLine'), 8, 0)),
	LASTNAMEINTERNALDENORM VARCHAR(30) AS (CAST(GET($1, 'LastNameInternalDenorm') AS VARCHAR(30))),
	ID NUMBER(8,0) AS (TO_NUMBER(GET($1, 'ID'), 8, 0)),
	CREATEUSERID NUMBER(8,0) AS (TO_NUMBER(GET($1, 'CreateUserID'), 8, 0)),
	MARITALSTATUSINTERNAL NUMBER(38,0) AS (TO_NUMBER(GET($1, 'MaritalStatusInternal'))),
	POLICYLINE NUMBER(8,0) AS (TO_NUMBER(GET($1, 'PolicyLine'), 8, 0)),
	BEANVERSION NUMBER(38,0) AS (TO_NUMBER(GET($1, 'BeanVersion'))),
	LICENSESTATEINTERNAL NUMBER(38,0) AS (TO_NUMBER(GET($1, 'LicenseStateInternal'))),
	UPDATEUSERID NUMBER(38,0) AS (TO_NUMBER(GET($1, 'UpdateUserID'))),
	FIRSTNAMEINTERNALDENORM VARCHAR(30) AS (CAST(GET($1, 'FirstNameInternalDenorm') AS VARCHAR(30))),
	QUICKQUOTENUMBER NUMBER(38,0) AS (TO_NUMBER(GET($1, 'QuickQuoteNumber'))),
	WORKERSCOMPLINE NUMBER(8,0) AS (TO_NUMBER(GET($1, 'WorkersCompLine'), 8, 0)),
	RELATIONSHIP VARCHAR(1333) AS (CAST(GET($1, 'Relationship') AS VARCHAR(1333))),
	INCLUDED NUMBER(38,0) AS (TO_NUMBER(GET($1, 'Included'))),
	BRANCHID NUMBER(8,0) AS (TO_NUMBER(GET($1, 'BranchID'), 8, 0)),
	CLASSCODEID NUMBER(8,0) AS (TO_NUMBER(GET($1, 'ClassCodeID'), 8, 0)),
	COMPANYNAMEINTERNALDENORM VARCHAR(255) AS (CAST(GET($1, 'CompanyNameInternalDenorm') AS VARCHAR(255))),
	DONOTORDERMVR BOOLEAN AS (CAST(GET($1, 'DoNotOrderMVR') AS BOOLEAN)),
	PARTICLEINTERNAL VARCHAR(255) AS (CAST(GET($1, 'ParticleInternal') AS VARCHAR(255))),
	PUBLICID VARCHAR(64) AS (CAST(GET($1, 'PublicID') AS VARCHAR(64))),
	COMPANYNAMEINTERNAL VARCHAR(255) AS (CAST(GET($1, 'CompanyNameInternal') AS VARCHAR(255))),
	APPLICABLEGOODDRIVERDISCOUNT BOOLEAN AS (CAST(GET($1, 'ApplicableGoodDriverDiscount') AS BOOLEAN)),
	INLANDMARINELINE NUMBER(8,0) AS (TO_NUMBER(GET($1, 'InlandMarineLine'), 8, 0)),
	ACCOUNTCONTACTROLE NUMBER(8,0) AS (TO_NUMBER(GET($1, 'AccountContactRole'), 8, 0)),
	CREATETIME TIMESTAMP_NTZ(9) AS (CAST(GET($1, 'CreateTime') AS TIMESTAMP_NTZ(9))),
	COMPANYNAMEKANJIINTERNAL VARCHAR(255) AS (CAST(GET($1, 'CompanyNameKanjiInternal') AS VARCHAR(255))),
	NUMBEROFVIOLATIONS NUMBER(38,0) AS (TO_NUMBER(GET($1, 'NumberOfViolations'))),
	LASTNAMEKANJIINTERNALDENORM VARCHAR(30) AS (CAST(GET($1, 'LastNameKanjiInternalDenorm') AS VARCHAR(30))),
	LASTNAMEKANJIINTERNAL VARCHAR(30) AS (CAST(GET($1, 'LastNameKanjiInternalDenorm') AS VARCHAR(30))),
	BUSINESSOWNERSLINE NUMBER(8,0) AS (TO_NUMBER(GET($1, 'BusinessOwnersLine'), 8, 0)),
	EFFECTIVEDATE TIMESTAMP_NTZ(9) AS (CAST(GET($1, 'EffectiveDate') AS TIMESTAMP_NTZ(9))),
	EXPIRATIONDATE TIMESTAMP_NTZ(9) AS (CAST(GET($1, 'EffectiveDate') AS TIMESTAMP_NTZ(9))),
	CONTACTDENORM NUMBER(8,0) AS (TO_NUMBER(GET($1, 'ContactDenorm'), 8, 0)),
	RELATIONSHIPTITLEINTERNAL NUMBER(38,0) AS (TO_NUMBER(GET($1, 'RelationshipTitleInternal'))),
	LASTNAMEINTERNAL VARCHAR(30) AS (CAST(GET($1, 'LastNameInternal') AS VARCHAR(30))),
	BUSINESSAUTOLINE NUMBER(8,0) AS (TO_NUMBER(GET($1, 'BusinessAutoLine'), 8, 0)),
	PERSONALAUTOLINE NUMBER(8,0) AS (TO_NUMBER(GET($1, 'PersonalAutoLine'), 8, 0)),
	NUMBEROFACCIDENTS NUMBER(38,0) AS (TO_NUMBER(GET($1, 'NumberOfAccidents'))),
	SEQNUMBER NUMBER(38,0) AS (TO_NUMBER(GET($1, 'SeqNumber'))),
	ARCHIVEPARTITION NUMBER(8,0) AS (TO_NUMBER(GET($1, 'ArchivePartition'), 8, 0)),
	CHANGETYPE NUMBER(38,0) AS (TO_NUMBER(GET($1, 'ChangeType'))),
	BASEDONID NUMBER(8,0) AS (TO_NUMBER(GET($1, 'BasedOnID'), 8, 0)),
	FIRSTNAMEKANJIINTERNALDENORM VARCHAR(30) AS (CAST(GET($1, 'FirstNameKanjiInternalDenorm') AS VARCHAR(30))),
	FIRSTNAMEINTERNAL VARCHAR(30) AS (CAST(GET($1, 'FirstNameInternal') AS VARCHAR(30))),
	FIRSTNAMEKANJIINTERNAL VARCHAR(30) AS (CAST(GET($1, 'FirstNameKanjiInternal') AS VARCHAR(30))),
	SUBTYPE NUMBER(38,0) AS (TO_NUMBER(GET($1, 'Subtype'))))
location=@CSVLOADING/
auto_refresh=false
pattern='.*dbo_PC_POLICYCONTACTROLE_.*'
file_format=(TYPE=PARQUET NULL_IF=())
;