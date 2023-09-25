create or replace external table ET_PC_POLICYPERIOD(
	BASEDONDATE TIMESTAMP_NTZ(9) AS (CAST(GET($1, 'BasedOnDate') AS TIMESTAMP_NTZ(9))),
	VALIDQUOTE BOOLEAN AS (CAST(GET($1, 'ValidQuote') AS BOOLEAN)),
	TOTALPREMIUMRPT NUMBER(38,0) AS (TO_NUMBER(GET($1, 'TotalPremiumRPT'))),
	TOTALPREMIUMRPT_CUR NUMBER(38,0) AS (TO_NUMBER(GET($1, 'TotalPremiumRPT_cur'))),
	MINIMUMPREMIUM NUMBER(38,0) AS (TO_NUMBER(GET($1, 'MinimumPremium'))),
	LOCKED BOOLEAN AS (CAST(GET($1, 'Locked') AS BOOLEAN)),
	EDITEFFECTIVEDATE TIMESTAMP_NTZ(9) AS (CAST(GET($1, 'EditEffectiveDate') AS TIMESTAMP_NTZ(9))),
	VALIDREINSURANCE BOOLEAN AS (CAST(GET($1, 'ValidReinsurance') AS BOOLEAN)),
	SERIESCHECKINGPATTERNCODE VARCHAR(16777216) AS (CAST(GET($1, 'SeriesCheckingPatternCode') AS VARCHAR(16777216))),
	ARCHIVESTATE NUMBER(38,0) AS (TO_NUMBER(GET($1, 'ArchiveState'))),
	ARCHIVESCHEMAINFO NUMBER(38,0) AS (TO_NUMBER(GET($1, 'ArchiveSchemaInfo'))),
	PNICONTACTDENORM NUMBER(38,0) AS (TO_NUMBER(GET($1, 'PNIContactDenorm'))),
	LOCATIONAUTONUMBERSEQ NUMBER(38,0) AS (TO_NUMBER(GET($1, 'LocationAutoNumberSeq'))),
	EDITLOCKED BOOLEAN AS (CAST(GET($1, 'EditLocked') AS BOOLEAN)),
	UPDATETIME TIMESTAMP_NTZ(9) AS (CAST(GET($1, 'UpdateTime') AS TIMESTAMP_NTZ(9))),
	RATEASOFDATE TIMESTAMP_NTZ(9) AS (CAST(GET($1, 'RateAsOfDate') AS TIMESTAMP_NTZ(9))),
	JOBID NUMBER(38,0) AS (TO_NUMBER(GET($1, 'JobID'))),
	ID NUMBER(38,0) AS (TO_NUMBER(GET($1, 'ID'))),
	SINGLECHECKINGPATTERNCODE VARCHAR(16777216) AS (CAST(GET($1, 'SingleCheckingPatternCode') AS VARCHAR(16777216))),
	UWCOMPANY NUMBER(38,0) AS (TO_NUMBER(GET($1, 'UWCompany'))),
	BILLINGMETHOD NUMBER(38,0) AS (TO_NUMBER(GET($1, 'BillingMethod'))),
	PERIODID NUMBER(38,0) AS (TO_NUMBER(GET($1, 'PeriodID'))),
	TRANSACTIONPREMIUMRPT NUMBER(38,0) AS (TO_NUMBER(GET($1, 'TransactionPremiumRPT'))),
	ASSIGNEDRISK BOOLEAN AS (CAST(GET($1, 'AssignedRisk') AS BOOLEAN)),
	EXCLUDEREASON VARCHAR(16777216) AS (CAST(GET($1, 'ExcludeReason') AS VARCHAR(16777216))),
	TRANSACTIONPREMIUMRPT_CUR NUMBER(38,0) AS (TO_NUMBER(GET($1, 'TransactionPremiumRPT_cur'))),
	CREATEUSERID NUMBER(38,0) AS (TO_NUMBER(GET($1, 'CreateUserID'))),
	ARCHIVEFAILUREID NUMBER(38,0) AS (TO_NUMBER(GET($1, 'ArchiveFailureID'))),
	ALLOWGAPSBEFORE BOOLEAN AS (CAST(GET($1, 'AllowGapsBefore') AS BOOLEAN)),
	QUOTEHIDDEN BOOLEAN AS (CAST(GET($1, 'QuoteHidden') AS BOOLEAN)),
	BEANVERSION NUMBER(38,0) AS (TO_NUMBER(GET($1, 'BeanVersion'))),
	FAILEDOOSEVALIDATION BOOLEAN AS (CAST(GET($1, 'FailedOOSEValidation') AS BOOLEAN)),
	RETIRED NUMBER(38,0) AS (TO_NUMBER(GET($1, 'Retired'))),
	BRANCHNAME VARCHAR(16777216) AS (CAST(GET($1, 'BranchName') AS VARCHAR(16777216))),
	PREEMPTED BOOLEAN AS (CAST(GET($1, 'Preempted') AS BOOLEAN)),
	UPDATEUSERID NUMBER(38,0) AS (TO_NUMBER(GET($1, 'UpdateUserID'))),
	FUTUREPERIODS BOOLEAN AS (CAST(GET($1, 'FuturePeriods') AS BOOLEAN)),
	PRIMARYINSUREDNAMEDENORM VARCHAR(16777216) AS (CAST(GET($1, 'PrimaryInsuredNameDenorm') AS VARCHAR(16777216))),
	CANCELLATIONDATE TIMESTAMP_NTZ(9) AS (CAST(GET($1, 'CancellationDate') AS TIMESTAMP_NTZ(9))),
	MODELNUMBER NUMBER(38,0) AS (TO_NUMBER(GET($1, 'ModelNumber'))),
	TEMPORARYBRANCH BOOLEAN AS (CAST(GET($1, 'TemporaryBranch') AS BOOLEAN)),
	PRIMARYINSUREDNAME VARCHAR(16777216) AS (CAST(GET($1, 'PrimaryInsuredName') AS VARCHAR(16777216))),
	SEGMENT NUMBER(38,0) AS (TO_NUMBER(GET($1, 'Segment'))),
	TERMNUMBER NUMBER(38,0) AS (TO_NUMBER(GET($1, 'TermNumber'))),
	DEPOSITOVERRIDEPCT NUMBER(38,0) AS (TO_NUMBER(GET($1, 'DepositOverridePct'))),
	POLICYTERMID NUMBER(38,0) AS (TO_NUMBER(GET($1, 'PolicyTermID'))),
	WAIVEDEPOSITCHANGE BOOLEAN AS (CAST(GET($1, 'WaiveDepositChange') AS BOOLEAN)),
	PERIODSTART TIMESTAMP_NTZ(9) AS (CAST(GET($1, 'PeriodStart') AS TIMESTAMP_NTZ(9))),
	PRODUCERCODEOFRECORDID NUMBER(38,0) AS (TO_NUMBER(GET($1, 'ProducerCodeOfRecordID'))),
	DONOTPURGE BOOLEAN AS (CAST(GET($1, 'DoNotPurge') AS BOOLEAN)),
	PUBLICID VARCHAR(16777216) AS (CAST(GET($1, 'PublicID') AS VARCHAR(16777216))),
	ALTBILLINGACCOUNTNUMBER VARCHAR(16777216) AS (CAST(GET($1, 'AltBillingAccountNumber') AS VARCHAR(16777216))),
	TOTALCOSTRPT NUMBER(38,0) AS (TO_NUMBER(GET($1, 'TotalCostRPT'))),
	WRITTENDATE TIMESTAMP_NTZ(9) AS (CAST(GET($1, 'WrittenDate') AS TIMESTAMP_NTZ(9))),
	TOTALCOSTRPT_CUR NUMBER(38,0) AS (TO_NUMBER(GET($1, 'TotalCostRPT_cur'))),
	CREATETIME TIMESTAMP_NTZ(9) AS (CAST(GET($1, 'CreateTime') AS TIMESTAMP_NTZ(9))),
	MOSTRECENTMODEL BOOLEAN AS (CAST(GET($1, 'MostRecentModel') AS BOOLEAN)),
	POLICYID NUMBER(38,0) AS (TO_NUMBER(GET($1, 'PolicyID'))),
	EXCLUDEDFROMARCHIVE BOOLEAN AS (CAST(GET($1, 'ExcludedFromArchive') AS BOOLEAN)),
	ALLOCATIONOFREMAINDER NUMBER(38,0) AS (TO_NUMBER(GET($1, 'AllocationOfRemainder'))),
	OVERRIDEBILLINGALLOCATION BOOLEAN AS (CAST(GET($1, 'OverrideBillingAllocation') AS BOOLEAN)),
	ARCHIVEFAILUREDETAILSID NUMBER(38,0) AS (TO_NUMBER(GET($1, 'ArchiveFailureDetailsID'))),
	MODELDATE TIMESTAMP_NTZ(9) AS (CAST(GET($1, 'ModelDate') AS TIMESTAMP_NTZ(9))),
	INVOICESTREAMCODE VARCHAR(16777216) AS (CAST(GET($1, 'InvoiceStreamCode') AS VARCHAR(16777216))),
	MODELNUMBERINDEX VARCHAR(16777216) AS (CAST(GET($1, 'ModelNumberIndex') AS VARCHAR(16777216))),
	BASESTATE NUMBER(38,0) AS (TO_NUMBER(GET($1, 'BaseState'))),
	MOSTRECENTMODELINDEX VARCHAR(16777216) AS (CAST(GET($1, 'MostRecentModelIndex') AS VARCHAR(16777216))),
	ARCHIVEPARTITION NUMBER(38,0) AS (TO_NUMBER(GET($1, 'ArchivePartition'))),
	CUSTOMBILLING BOOLEAN AS (CAST(GET($1, 'CustomBilling') AS BOOLEAN)),
	TRANSACTIONCOSTRPT NUMBER(38,0) AS (TO_NUMBER(GET($1, 'TransactionCostRPT'))),
	BRANCHNUMBER NUMBER(38,0) AS (TO_NUMBER(GET($1, 'BranchNumber'))),
	FAILEDOOSEEVALUATION BOOLEAN AS (CAST(GET($1, 'FailedOOSEEvaluation') AS BOOLEAN)),
	DEPOSITCOLLECTED NUMBER(38,0) AS (TO_NUMBER(GET($1, 'DepositCollected'))),
	TRANSACTIONCOSTRPT_CUR NUMBER(38,0) AS (TO_NUMBER(GET($1, 'TransactionCostRPT_cur'))),
	DEPOSITCOLLECTED_CUR NUMBER(38,0) AS (TO_NUMBER(GET($1, 'DepositCollected_cur'))),
	BASEDONID NUMBER(38,0) AS (TO_NUMBER(GET($1, 'BasedOnID'))),
	LOCKINGCOLUMN NUMBER(38,0) AS (TO_NUMBER(GET($1, 'LockingColumn'))),
	REFUNDCALCMETHOD NUMBER(38,0) AS (TO_NUMBER(GET($1, 'RefundCalcMethod'))),
	ARCHIVEDATE TIMESTAMP_NTZ(9) AS (CAST(GET($1, 'ArchiveDate') AS TIMESTAMP_NTZ(9))),
	BILLIMMEDIATELYPERCENTAGE NUMBER(38,0) AS (TO_NUMBER(GET($1, 'BillImmediatelyPercentage'))),
	STATUS NUMBER(38,0) AS (TO_NUMBER(GET($1, 'Status'))),
	DEPOSITAMOUNT NUMBER(38,0) AS (TO_NUMBER(GET($1, 'DepositAmount'))),
	DEPOSITAMOUNT_CUR NUMBER(38,0) AS (TO_NUMBER(GET($1, 'DepositAmount_cur'))),
	PERIODEND TIMESTAMP_NTZ(9) AS (CAST(GET($1, 'PeriodEnd') AS TIMESTAMP_NTZ(9))),
	PREFERREDCOVERAGECURRENCY NUMBER(38,0) AS (TO_NUMBER(GET($1, 'PreferredCoverageCurrency'))),
	POLICYNUMBER VARCHAR(16777216) AS (CAST(GET($1, 'PolicyNumber') AS VARCHAR(16777216))),
	PREFERREDSETTLEMENTCURRENCY NUMBER(38,0) AS (TO_NUMBER(GET($1, 'PreferredSettlementCurrency'))))
location=@CSVLOADING/
auto_refresh=false
pattern='.*dbo_PC_POLICYPERIOD_.*'
file_format=(TYPE=PARQUET NULL_IF=())
;