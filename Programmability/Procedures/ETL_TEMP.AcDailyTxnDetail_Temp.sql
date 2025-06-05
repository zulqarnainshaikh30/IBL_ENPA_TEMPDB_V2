SET QUOTED_IDENTIFIER, ANSI_NULLS ON
GO


-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================

CREATE PROCEDURE [ETL_TEMP].[AcDailyTxnDetail_Temp] 
	-- Add the parameters for the stored procedure here
	
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

	SET DATEFORMAT DMY

	BEGIN TRY
    -- Insert statements for procedure here
TRUNCATE TABLE TempAcDailyTxnDetail
	
SET DATEFORMAT DMY
DECLARE  @vEffectivefrom  Int SET @vEffectiveFrom=(SELECT TimeKey FROM IBL_ENPA_DB_V2..SYSDATAMATRIX WHERE CurrentStatus='Y')
DECLARE @TimeKey  Int SET @TimeKey=(SELECT TimeKey FROM IBL_ENPA_DB_V2..SYSDATAMATRIX WHERE CurrentStatus='Y')
DECLARE @DATE AS DATE =(SELECT Date FROM IBL_ENPA_DB_V2..SYSDATAMATRIX WHERE CurrentStatus='Y')
 
INSERT INTO TempAcDailyTxnDetail
(	    
[Branchcode]
,[CustomerID]
,[CustomerAcID]
,[AccountEntityId]
,[TxnDate]
,[TxnType]
,[TxnSubType]
,[TxnTime]
,[CurrencyAlt_Key]
,[CurrencyConvRate]
,[TxnAmount]
,[TxnAmountInCurrency]
,[ExtDate]
,[TxnRefNo]
,[TxnValueDate]
,[MnemonicCode]
,[Particular]
,[AuthorisationStatus]
,[CreatedBy]
,[DateCreated]
,[ModifiedBy]
,[DateModified]
,[ApprovedBy]
,[DateApproved]
,[Remark]
,[TrueCredit]
,[IsProcessed]
,[Balance]
)

----LMS DATA------
SELECT		
NULL AS [Branchcode]  
,ACBD.REFCUSTOMERID AS [CustomerID]  
,TXN.AccountID AS [CustomerAcID]  
,ACBD.[AccountEntityId] AS AccountEntityId  
,TXN.TxnDate AS [TxnDate]  
,CASE WHEN TXN.TxnType='DR' then 'DEBIT'
	 WHEN TXN.TxnType='CR' THEN 'CREDIT' end
AS  [TxnType]   
,CASE WHEN  TXN.TxnType='DR' AND TXN.TxnSubType ='IC' THEN 'INTEREST' 
	 WHEN TXN.TxnType='DR' THEN 'OTHER DEBIT' 
	 ELSE TXN.TxnSubType END  AS [TxnSubType]   
,NULL AS [TxnTime]  
,DC.CurrencyAlt_Key AS [CurrencyAlt_Key]  
,DCC.ConvRate AS [CurrencyConvRate]   
,sum(cast(TXN.TxnAmountINR as decimal(18,2))) AS [TxnAmount]  
,sum(cast(TXN.TxnAmountinCurrency as decimal(18,2))) AS [TxnAmountInCurrency]    
,@DATE AS  [ExtDate]
,TxnID AS [TxnRefNo]  
,NULL AS [TxnValueDate]  
,NULL AS [MnemonicCode]  
,TXN.TxnParticulars AS [Particular]   
,NULL AS [AuthorisationStatus]
,'SSISUSER' AS [CreatedBy]  
,GETDATE() as [DateCreated]
,NULL AS [ModifiedBy]
,NULL AS [DateModified]
,NULL AS [ApprovedBy]
,NULL AS [DateApproved]
,NULL AS  [Remark]  
,'Y' AS [TrueCredit] 
,NULL AS [IsProcessed]
,NULL AS Balance   
 FROM IBL_ENPA_STGDB_V2..TXN_ALL_SOURCE_SYSTEM TXN
LEFT JOIN TempAdvAcBasicDetail ACBD  ON ACBD.CustomerAcID=TXN.AccountID
LEFT JOIN UTKS_MISDB.dbo.DimCurrency DC	ON LTRIM(RTRIM(ISNULL(DC.CurrencyCode,'')))=TXN.TxnCurrency
and DC.EffectiveFromTimeKey<=@TimeKey AND DC.EffectiveToTimeKey>=@TimeKey
LEFT join UTKS_MISDB.dbo.DimCurCovRate DCC on DCC.CurrencyCode= DC.CurrencyCode AND DCC.ConvDate=@DATE
and DCC.EffectiveFromTimeKey<=@TimeKey AND DCC.EffectiveToTimeKey>=@TimeKey
 group by TXN.TxnParticulars,TxnID,ACBD.REFCUSTOMERID,TXN.AccountID,ACBD.[AccountEntityId]
,TXN.TxnDate,TXN.TxnType,TXN.TxnSubType   ,DC.CurrencyAlt_Key,DCC.ConvRate




UPDATE A SET AccountEntityId=B.AccountEntityId,CUSTOMERID=B.REFCUSTOMERID
FROM TempACDailyTXNDetail A
INNER JOIN TempAdvNFAcBasicDetail B
ON A.CustomerAcID=B.CustomerAcID

UPDATE TempAcDailyTxnDetail set TxnSubType='RECOVERY' WHERE TxnType='CREDIT'

UPDATE A SET TrueCredit='N'
FROM TempAcDailyTxnDetail A
INNER JOIN UTKS_STGDB.dbo.LMS_Txn_STG B
ON A.CustomerAcID=B.AccountID
and A.TxnRefNo = B.TxnID
WHERE B.InValidIdentifier IN('Y','YES')

END TRY
BEGIN CATCH

Update UTKS_STGDB.dbo.PACKAGE_AUDIT 
SET ExecutionStatus = -1 
where Tablename = 'Temp_AcDailyTxnDetail' 
and PackageName = 'StageToTempDB' 
and Execution_Date = cast(GETDATE() as date)

Update [UTKS_MISDB].dbo.BANDAUDITSTATUS 
set BandStatus = 'Failed' 
where BandName in ('StageToTemp')

RAISERROR('Insert Failed',16,2)

END CATCH

END
GO