create or replace view BA_STG_VW(
	ID,
	TRANSACTION_PRO_DATE,
	TRANSACTION_TYPE_CODE,
	TRANSACTION_CODE,
	WRITTEN_PREMIUM_AMT,
	CHARGE_TYPECODE,
	TOTALPREMIUMRPT,
	TOTALREPORTEDPREMIUM,
	PCTL_TYPECODE,
	TAX_AMOUNT,
	FEE_AMOUNT,
	SURCHARGE_AMOUNT,
	PREM_BASIS_AMT,
	AMOUNT,
	ACTUALAMOUNT,
	TRANSACTION_AMOUNT,
	ACCTG_PRD_ID,
	TRANS_EFF_DATE,
	TRANS_EXP_DATE,
	RISK_TYPE_CODE,
	PREM_FULLY_EARNED_FL,
	CURRENCY_CODE,
	INFORCE_PREM_AMOUNT
) as
WITH CTE_TRANSACTION_TYPE_CODE (TRANSACTION_TYPE_CODE,PC_BACOST_ID) AS
--CALCULATION OF TRANSACTION_TYPE_CODE
(SELECT dl_pc_ext.et_pctl_bacost.TYPECODE as TRANSACTION_TYPE_CODE,
dl_pc_ext.et_pc_BACOST.ID AS PC_BACOST_ID
from dl_pc_ext.et_pctl_bacost left outer join dl_pc_ext.et_pc_BACOST
on dl_pc_ext.et_pctl_bacost.id = dl_pc_ext.et_pc_BACOST.SUBTYPE),
CTE_TRANSACTION_CODE (TRANSACTION_CODE,PC_JOB_ID) AS
--CALCULATION OF TRANSACTION_CODE
(select dl_pc_ext.et_pctl_job.TYPECODE as TRANSACTION_CODE, dl_pc_ext.et_pc_JOB.ID AS PC_JOB_ID
from dl_pc_ext.et_pctl_job left outer join dl_pc_ext.et_pc_JOB 
on dl_pc_ext.et_pctl_job.id = dl_pc_ext.et_pc_JOB.SUBTYPE),
CTE_CHARGE_TYPECODE(CHARGE_TYPECODE,PC_BOPCOST_ID) AS
--CALCULATION OF CHARGE_TYPECODE
(select  dl_pc_ext.et_pctl_chargepattern.TYPECODE as CHARGE_TYPECODE,dl_pc_ext.et_pc_BOPCOST.ID AS PC_BOPCOST_ID
from dl_pc_ext.et_pctl_chargepattern left outer join dl_pc_ext.et_pc_BOPCOST
on dl_pc_ext.et_pctl_chargepattern.id = dl_pc_ext.et_pc_BOPCOST.CHARGEPATTERN),
CTE_CURRENCY_CODE (CURRENCY_CODE,PC_BATRANSACTION_ID) AS
--CALCULATION OF CHARGE_TYPECODE
(SELECT dl_pc_ext.et_pctl_currency.TYPECODE AS CURRENCY_CODE,dl_pc_ext.et_pc_BATRANSACTION.ID AS PC_BATRANSACTION_ID
FROM dl_pc_ext.et_pctl_currency LEFT OUTER JOIN dl_pc_ext.et_pc_BATRANSACTION
ON dl_pc_ext.et_pctl_currency.ID = dl_pc_ext.et_pc_BATRANSACTION.AMOUNT_CUR),
CTE_PCTL_TYPECODE (PCTL_TYPECODE,PC_POLICYPERIOD_ID) AS
--CALCULATION OF PCTL_TYPECODE
(SELECT dl_pc_ext.et_pctl_policyperiodstatus.TYPECODE as PCTL_TYPECODE,dl_pc_ext.et_pc_POLICYPERIOD.ID AS PC_POLICYPERIOD_ID
FROM dl_pc_ext.et_pctl_policyperiodstatus LEFT OUTER JOIN dl_pc_ext.et_pc_POLICYPERIOD
ON dl_pc_ext.et_pctl_policyperiodstatus.ID = dl_pc_ext.et_pc_POLICYPERIOD.STATUS)
SELECT
dl_pc_ext.et_pc_BATRANSACTION.ID AS ID,
cast(dl_pc_ext.et_pc_JOB.CLOSEDATE AS DATETIME) as TRANSACTION_PRO_DATE,
TRANSACTION_TYPE_CODE, 
TRANSACTION_CODE,
CASE WHEN (CTE_CHARGE_TYPECODE.CHARGE_TYPECODE='Premium' AND dl_pc_ext.et_pc_BATRANSACTION.CHARGED=1) THEN dl_pc_ext.et_pc_BATRANSACTION.AMOUNT ELSE 0 END AS WRITTEN_PREMIUM_AMT,
CHARGE_TYPECODE,
dl_pc_ext.et_pc_POLICYPERIOD.TOTALPREMIUMRPT AS TOTALPREMIUMRPT,
dl_pc_ext.et_pc_POLICYTERM.TOTALREPORTEDPREMIUM AS TOTALREPORTEDPREMIUM,
PCTL_TYPECODE,
CASE WHEN (CTE_CHARGE_TYPECODE.CHARGE_TYPECODE = 'Taxes') THEN dl_pc_ext.et_pc_BATRANSACTION.AMOUNT ELSE 0 END AS TAX_AMOUNT,
CASE WHEN(CTE_CHARGE_TYPECODE.CHARGE_TYPECODE='InstallmentFee' OR CTE_CHARGE_TYPECODE.CHARGE_TYPECODE='ReinstatementFee') THEN
dl_pc_ext.et_pc_BATRANSACTION.AMOUNT ELSE 0 END AS FEE_AMOUNT,
CASE WHEN (CTE_CHARGE_TYPECODE.CHARGE_TYPECODE='Surcharges') THEN dl_pc_ext.et_pc_BATRANSACTION.AMOUNT ELSE 0 END AS SURCHARGE_AMOUNT,
CAST(dl_pc_ext.et_pc_BACOST.BASIS AS DECIMAL) AS PREM_BASIS_AMT,
CAST(dl_pc_ext.et_pc_BATRANSACTION.AMOUNT AS DECIMAL) AS AMOUNT,
CAST(dl_pc_ext.et_pc_BACOST.ACTUALAMOUNT AS DECIMAL) AS ACTUALAMOUNT,
dl_pc_ext.et_pc_BATRANSACTION.AMOUNT as TRANSACTION_AMOUNT,
TO_CHAR(DL_PC_EXT.et_pc_batransaction.WRITTENDATE,'MMYYYY')::NUMBER AS ACCTG_PRD_ID,
CAST(dl_pc_ext.et_pc_BATRANSACTION.EFFDATE AS DATETIME) AS TRANS_EFF_DATE,
cast(dl_pc_ext.et_pc_BATRANSACTION.EXPDATE AS DATETIME) AS TRANS_EXP_DATE,
'BA' AS RISK_TYPE_CODE,
CASE WHEN (dl_pc_ext.et_pc_BATRANSACTION.Written = 1 AND dl_pc_ext.et_pc_BATRANSACTION.TOBEACCRUED =1) THEN 'Y' ELSE 'N' END AS PREM_FULLY_EARNED_FL,
CURRENCY_CODE,
CASE WHEN (CTE_CHARGE_TYPECODE.CHARGE_TYPECODE='Premium' AND dl_pc_ext.et_pc_BATRANSACTION.CHARGED=1 )THEN dl_pc_ext.et_pc_BATRANSACTION.AMOUNT ELSE 0 END AS INFORCE_PREM_AMOUNT
FROM dl_pc_ext.et_pc_BATRANSACTION 
LEFT OUTER JOIN dl_pc_ext.et_pc_BACOST
ON dl_pc_ext.et_pc_BATRANSACTION.BACOST  = dl_pc_ext.et_pc_BACOST.ID
LEFT OUTER JOIN dl_pc_ext.et_pc_POLICYPERIOD
ON dl_pc_ext.et_pc_BATRANSACTION.BRANCHID = dl_pc_ext.et_pc_POLICYPERIOD.ID
LEFT OUTER JOIN dl_pc_ext.et_pc_JOB 
ON dl_pc_ext.et_pc_JOB.ID = dl_pc_ext.et_pc_POLICYPERIOD.JOBID
LEFT OUTER JOIN dl_pc_ext.et_pc_BUSINESSVEHICLECOV ON 
dl_pc_ext.et_pc_BACOST.BRANCHID = dl_pc_ext.et_pc_BUSINESSVEHICLECOV.BRANCHID AND
dl_pc_ext.et_pc_BACOST.BUSINESSVEHICLECOV = dl_pc_ext.et_pc_BUSINESSVEHICLECOV.FIXEDID AND
dl_pc_ext.et_pc_POLICYPERIOD.EDITEFFECTIVEDATE >= COALESCE(dl_pc_ext.et_pc_BUSINESSVEHICLECOV.EFFECTIVEDATE,dl_pc_ext.et_pc_POLICYPERIOD.PERIODSTART) AND 
dl_pc_ext.et_pc_POLICYPERIOD.EDITEFFECTIVEDATE < COALESCE(dl_pc_ext.et_pc_BUSINESSVEHICLECOV.EXPIRATIONDATE,dl_pc_ext.et_pc_POLICYPERIOD.PERIODEND)
LEFT OUTER JOIN dl_pc_ext.et_pc_BUSINESSVEHICLE ON
dl_pc_ext.et_pc_BACOST.BRANCHID = dl_pc_ext.et_pc_BUSINESSVEHICLE.BRANCHID AND
COALESCE(dl_pc_ext.et_pc_BACOST.BUSINESSVEHICLE,dl_pc_ext.et_pc_BUSINESSVEHICLECOV.VEHICLE ) = dl_pc_ext.et_pc_BUSINESSVEHICLE.FIXEDID AND
dl_pc_ext.et_pc_POLICYPERIOD.EDITEFFECTIVEDATE >= COALESCE(dl_pc_ext.et_pc_BUSINESSVEHICLE.EFFECTIVEDATE,dl_pc_ext.et_pc_POLICYPERIOD.PERIODSTART) AND
dl_pc_ext.et_pc_POLICYPERIOD.EDITEFFECTIVEDATE < COALESCE( dl_pc_ext.et_pc_BUSINESSVEHICLE.EXPIRATIONDATE,dl_pc_ext.et_pc_POLICYPERIOD.PERIODEND)
LEFT OUTER JOIN dl_pc_ext.et_pc_POLICYLOCATION ON
dl_pc_ext.et_pc_BUSINESSVEHICLE.BRANCHID = dl_pc_ext.et_pc_POLICYLOCATION.BRANCHID AND
dl_pc_ext.et_pc_BUSINESSVEHICLE.LOCATION  = dl_pc_ext.et_pc_POLICYLOCATION.FIXEDID AND
dl_pc_ext.et_pc_POLICYPERIOD.EDITEFFECTIVEDATE >= COALESCE(dl_pc_ext.et_pc_POLICYLOCATION.EFFECTIVEDATE,dl_pc_ext.et_pc_POLICYPERIOD.PERIODSTART)AND
dl_pc_ext.et_pc_POLICYPERIOD.EDITEFFECTIVEDATE < COALESCE(dl_pc_ext.et_pc_POLICYLOCATION.EXPIRATIONDATE,dl_pc_ext.et_pc_POLICYPERIOD.PERIODEND)
LEFT OUTER JOIN dl_pc_ext.et_pc_EFFECTIVEDATEDFIELDS ON
dl_pc_ext.et_pc_BACOST.BRANCHID = dl_pc_ext.et_pc_EFFECTIVEDATEDFIELDS.BRANCHID AND
dl_pc_ext.et_pc_POLICYPERIOD.EDITEFFECTIVEDATE >= COALESCE(dl_pc_ext.et_pc_EFFECTIVEDATEDFIELDS.EFFECTIVEDATE,dl_pc_ext.et_pc_POLICYPERIOD.PERIODSTART) AND
dl_pc_ext.et_pc_POLICYPERIOD.EDITEFFECTIVEDATE < COALESCE(dl_pc_ext.et_pc_EFFECTIVEDATEDFIELDS.EXPIRATIONDATE,dl_pc_ext.et_pc_POLICYPERIOD.PERIODEND)
LEFT OUTER JOIN dl_pc_ext.et_pc_POLICYLOCATION as POLICYLOCATION_1 ON
dl_pc_ext.et_pc_EFFECTIVEDATEDFIELDS.BRANCHID = POLICYLOCATION_1.BRANCHID AND
dl_pc_ext.et_pc_EFFECTIVEDATEDFIELDS.PRIMARYLOCATION = POLICYLOCATION_1.FIXEDID AND
dl_pc_ext.et_pc_POLICYPERIOD.EDITEFFECTIVEDATE >= COALESCE(POLICYLOCATION_1.EFFECTIVEDATE,dl_pc_ext.et_pc_POLICYPERIOD.PERIODSTART)AND
dl_pc_ext.et_pc_POLICYPERIOD.EDITEFFECTIVEDATE < COALESCE(POLICYLOCATION_1.EXPIRATIONDATE,dl_pc_ext.et_pc_POLICYPERIOD.PERIODEND)
LEFT OUTER JOIN dl_pc_ext.et_pc_auditinformation ON
dl_pc_ext.et_pc_AUDITINFORMATION.ID = dl_pc_ext.et_pc_JOB.AUDITINFORMATIONID
    JOIN CTE_TRANSACTION_TYPE_CODE    ON (dl_pc_ext.et_pc_BATRANSACTION.BACost = CTE_TRANSACTION_TYPE_CODE.PC_BACOST_ID )
    LEFT OUTER JOIN CTE_TRANSACTION_CODE ON ( dl_pc_ext.et_pc_batransaction.ID = CTE_TRANSACTION_CODE.PC_JOB_ID)
    LEFT OUTER JOIN CTE_CHARGE_TYPECODE ON (dl_pc_ext.et_pc_BATRANSACTION.ID = CTE_CHARGE_TYPECODE.PC_BOPCOST_ID)
    LEFT OUTER JOIN CTE_CURRENCY_CODE ON(dl_pc_ext.et_pc_BATRANSACTION.ID =CTE_CURRENCY_CODE.PC_BATRANSACTION_ID)
    LEFT OUTER JOIN CTE_PCTL_TYPECODE    ON(dl_pc_ext.et_pc_BATRANSACTION.ID = CTE_PCTL_TYPECODE.PC_POLICYPERIOD_ID)
    LEFT OUTER JOIN dl_pc_ext.et_pc_POLICYTERM ON (dl_pc_ext.et_pc_BATRANSACTION.ID =dl_pc_ext.et_pc_POLICYTERM.ID )
    WHERE PCTL_TYPECODE IN ('Bound','AuditComplete') 
    ;