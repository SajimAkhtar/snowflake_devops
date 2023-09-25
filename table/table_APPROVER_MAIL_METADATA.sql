create or replace TABLE APPROVER_MAIL_METADATA (
	S_NO NUMBER(38,0),
	ENVIRONMENT VARCHAR(16777216),
	SUBJECT VARCHAR(16777216),
	MAIL_BODY VARCHAR(16777216),
	DESIGNATION VARCHAR(16777216),
	APPROVER1 VARCHAR(16777216),
	EMAIL1 VARCHAR(16777216),
	CONTACT_NO1 VARCHAR(16777216),
	APPROVER2 VARCHAR(16777216),
	EMAIL2 VARCHAR(16777216),
	CONTACT_NO2 VARCHAR(16777216),
	APPROVER3 VARCHAR(16777216),
	EMAIL3 VARCHAR(16777216),
	CONTACT_NO3 VARCHAR(16777216),
	INSERT_DT TIMESTAMP_NTZ(9),
	INSERT_BY VARCHAR(16777216),
	UPD_DT TIMESTAMP_NTZ(9),
	UPD_BY VARCHAR(16777216)
);