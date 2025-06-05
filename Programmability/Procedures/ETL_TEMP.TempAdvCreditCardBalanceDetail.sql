SET QUOTED_IDENTIFIER, ANSI_NULLS ON
GO



CREATE PROC [ETL_TEMP].[TempAdvCreditCardBalanceDetail]
AS

BEGIN

SET NOCOUNT ON;


	DECLARE  @vEffectivefrom  Int SET @vEffectiveFrom=(SELECT TimeKey FROM IBL_ENPA_DB_V2.[dbo].SYSDATAMATRIX WHERE CurrentStatus='C')
DECLARE @TimeKey  Int SET @TimeKey=(SELECT TimeKey FROM IBL_ENPA_DB_V2.[dbo].SYSDATAMATRIX WHERE CurrentStatus='C')
DECLARE @DATE AS DATE =(SELECT Date FROM IBL_ENPA_DB_V2.[dbo].SYSDATAMATRIX WHERE CurrentStatus='C')


--------------------------------------------------------------------------------------------------------------------------------------------------- 
  
  TRUNCATE TABLE IBL_ENPA_TEMPDB_V2.DBO.TempAdvCreditCardBalanceDetail

  INSERT INTO IBL_ENPA_TEMPDB_V2.dbo.TempAdvCreditCardBalanceDetail
  (
  EntityKey
	,AccountEntityId
	,CreditCardEntityId
	,Balance_POS
	,Balance_LOAN
	,Balance_INT
	,Balance_GST
	,Balance_FEES
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
	,C.AccountEntityId
	,C.CreditCardEntityId
	,TRY_CAST(A.Balance_POS AS DECIMAL(16,2))
	,TRY_CAST(A.Balance_LOAN AS DECIMAL(16,2))
	,TRY_CAST(A.Balance_INT AS DECIMAL(16,2))
	,TRY_CAST(A.Balance_GST AS DECIMAL(16,2))
	,TRY_CAST(A.Balance_FEES AS DECIMAL(16,2))
	,C.RefSystemAcId
	,NULL AuthorisationStatus
	,B.EffectiveFromTimeKey
	,B.EffectiveToTimeKey
	,B.CreatedBy
	,Getdate() DateCreated
	,NULL ModifiedBy
	,NULL DateModified
	,NULL ApprovedBy
	,NULL DateApproved
	,NULL D2Ktimestamp
	,NULL MocStatus
	,NULL MocDate
	--,'U' ISChanged
	From IBL_ENPA_STGDB_V2.dbo.[PISMO_CREDITCARDDETAILS] A
	INNER JOIN IBL_ENPA_TEMPDB_V2.dbo.TempAdvAcBasicDetail B ON A.CustomerAcID=B.CustomerACID
	INNER JOIN IBL_ENPA_TEMPDB_V2.dbo.TempAdvFacCreditCardDetail C ON A.CustomerAcID=C.RefSystemAcId

END
GO