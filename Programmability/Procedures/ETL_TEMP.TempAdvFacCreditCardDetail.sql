SET QUOTED_IDENTIFIER, ANSI_NULLS ON
GO



CREATE PROC [ETL_TEMP].[TempAdvFacCreditCardDetail]
AS

BEGIN

SET NOCOUNT ON;


	DECLARE  @vEffectivefrom  Int SET @vEffectiveFrom=(SELECT TimeKey FROM IBL_ENPA_DB_V2.[dbo].SYSDATAMATRIX WHERE CurrentStatus='C')
DECLARE @TimeKey  Int SET @TimeKey=(SELECT TimeKey FROM IBL_ENPA_DB_V2.[dbo].SYSDATAMATRIX WHERE CurrentStatus='C')
DECLARE @DATE AS DATE =(SELECT Date FROM IBL_ENPA_DB_V2.[dbo].SYSDATAMATRIX WHERE CurrentStatus='C')


--------------------------------------------------------------------------------------------------------------------------------------------------- 
  
  TRUNCATE TABLE IBL_ENPA_TEMPDB_V2.DBO.TempAdvFacCreditCardDetail

  INSERT INTO IBL_ENPA_TEMPDB_V2.DBO.TempAdvFacCreditCardDetail  ----have to take structure from AUSFB UAT
  (
    EntityKey
	,AccountEntityId
	,CreditCardEntityId
	,CorporateUCIC_ID
	,CorporateCustomerID
	,Liability
	--,MinimumAmountDue
	--,CD
	--,Bucket
	,DPD
	,RefSystemAcId
	,AuthorisationStatus
	,EffectiveFromTimeKey
	,EffectiveToTimeKey
	,CreatedBy
	,DateCreated
	,ModifiedBy
	,DateModified
	,ApprovedBy
	,DateApproved
	,D2Ktimestamp
	,MocStatus
	,MocDate
	--,ISChanged
  )

  Select 
	NULL EntityKey
	,B.AccountEntityId
	,NULL CreditCardEntityId
	,A.CorporateUCICID  CorporateUCIC_ID
	,A.CorporateCustomerID  CorporateCustomerID
	,TRY_CAST(A.Liability AS DECIMAL(9,2)) AS Liability
	--,TRY_CAST(A.MinimumAmountDue AS DECIMAL(9,2)) AS MinimumAmountDue
	--,A.CD
	--,A.CD_Bucket
	,TRY_CAST( ISNULL(A.DPD,0) AS INT) AS DPD
	,B.SystemACID  RefSystemAcId
	,NULL AuthorisationStatus
	,B.EffectiveFromTimeKey EffectiveFromTimeKey
	,B.EffectiveToTimeKey  EffectiveToTimeKey
	,B.CreatedBy  CreatedBy
	,Getdate() DateCreated
	,NULL ModifiedBy
	,NULL DateModified
	,NULL ApprovedBy
	,NULL DateApproved
	,NULL D2Ktimestamp
	,NULL MocStatus
	,NULL MocDate
	--,'U' ISChanged

	From IBL_ENPA_STGDB_V2.dbo.[ACCOUNT_ALL_SOURCE_SYSTEM] A
	INNER JOIN IBL_ENPA_TEMPDB_V2.dbo.TempAdvAcBasicDetail B ON A.CustomerAcID=B.CustomerACID
	WHERE A.SourceSystem LIKE '%VISIONPLUS%'
	
 /*********************************************************************************************************/
/*  Existing Customers Account Entity ID Update  */

UPDATE TEMP 
SET TEMP.CreditCardEntityId=MAIN.CreditCardEntityId
 
FROM  IBL_ENPA_TEMPDB_V2.DBO.[TempAdvFacCreditCardDetail] TEMP
INNER JOIN IBL_ENPA_DB_V2.[DBO].[AdvFacCreditCardDetail] MAIN ON TEMP.RefSystemAcId=MAIN.RefSystemAcId
WHERE MAIN.EffectiveToTimeKey=49999

--GO
/*********************************************************************************************************/
/*  New Customers Account Entity ID Update  */

DECLARE @CreditCardEntityId INT=0 
SELECT @CreditCardEntityId=MAX(CreditCardEntityId) FROM  IBL_ENPA_DB_V2.[dbo].[AdvFacCreditCardDetail] 
IF @CreditCardEntityId IS NULL  
BEGIN
SET @CreditCardEntityId=0
END
 
UPDATE TEMP 
SET TEMP.CreditCardEntityId=ACCT.CreditCardEntityId
 FROM IBL_ENPA_TEMPDB_V2.DBO.[TempAdvFacCreditCardDetail] TEMP
INNER JOIN (SELECT RefSystemAcId,(@CreditCardEntityId + ROW_NUMBER()OVER(ORDER BY (SELECT 1))) CreditCardEntityId
			FROM IBL_ENPA_TEMPDB_V2.DBO.[TempAdvFacCreditCardDetail]
			WHERE CreditCardEntityId=0 OR CreditCardEntityId IS NULL)ACCT ON TEMP.RefSystemAcId=ACCT.RefSystemAcId


/*********************************************************************************************************/

END
GO