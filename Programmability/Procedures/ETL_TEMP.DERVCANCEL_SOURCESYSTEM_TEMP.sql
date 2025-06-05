SET QUOTED_IDENTIFIER, ANSI_NULLS ON
GO
CREATE PROCEDURE [ETL_TEMP].[DERVCANCEL_SOURCESYSTEM_TEMP] 
	
AS
BEGIN
	
	SET NOCOUNT ON;

    
	

DECLARE  @vEffectivefrom  Int SET @vEffectiveFrom=(SELECT TimeKey FROM UTKS_MISDB.[dbo].Automate_Advances WHERE EXT_FLG='Y')
DECLARE @TimeKey  Int SET @TimeKey=(SELECT TimeKey FROM UTKS_MISDB.[dbo].Automate_Advances WHERE EXT_FLG='Y')
DECLARE @DATE AS DATE =(SELECT Date FROM UTKS_MISDB.[dbo].Automate_Advances WHERE EXT_FLG='Y')


TRUNCATE TABLE TEMPDERVCANCEL_SOURCESYSTEM

INSERT INTO TEMPDERVCANCEL_SOURCESYSTEM
			(
			[DateofData] 
	       ,[SourceSystem] 
	       ,[BranchCode] 
	       ,[UCIC_ID] 
	       ,[CustID] 
	       ,[CustomerName] 
	       ,[AcID] 
	      ,[DerivativeRefNo] 
	      ,[Duedate]  
	      ,[DueAmt] 
	      ,[Os_Amt] 
	      ,[POS] 
		  ,MTMIncomeAmt
		,CouponDate
		,CouponAmt
		,CouponOverDueSinceDt
		,OverdueCouponAmt
		,InstrumentName
		,OverdueSinceDt  
		 ,[AuthorisationStatus] 
         ,[EffectiveFromTimeKey] 
	     ,[EffectiveToTimeKey] 
	     ,[CreatedBy] 
	     ,[DateCreated]
	     ,[ModifiedBy] 
	     ,[DateModified] 
	     ,[ApprovedBy] 
	     ,[DateApproved]
			
		)

SELECT		A.[DateofData] 
	         ,[SourceSystem] 
	         ,[BranchCode] 
	         ,[UCIC_ID] 
	         , [CustID]                                                                                
	         ,[CustomerName] 
	         ,[AcID] 
	         ,[DerivativeRefNo] 
	        ,[Duedate]  
	        ,[DueAmt] 
	        ,[Os_Amt] 
	        ,[POS]  
			,MTMIncomeAmt
			,CouponDate
			,CouponAmt
			,CouponOverDueSinceDt
			,OverdueCouponAmt
			,InstrumentName
			,OverdueSinceDt 
	       ,NULL AuthorisationStatus
		 ,@vEffectivefrom EffectiveFromTimeKey
			,49999 EffectiveToTimeKey
			,'SSISUSER' CreatedBy
			,GETDATE() DateCreated
			,NULL ModifiedBy
			,NULL DateModified
			,NULL ApprovedBy
			,NULL DateApproved 
			
FROM IBL_ENPA_STGDB_V2.[dbo].CALYPSO_DERVCANCEL_STG  A



/*********************************************************************************************************/
/*  Existing Customers Customer Entity ID Update  */

UPDATE TEMP 
SET TEMP.DerivativeEntityID=MAIN.DerivativeEntityId
FROM  UTKS_TEMPDB.DBO.TEMPDERVCANCEL_SOURCESYSTEM TEMP
INNER JOIN UTKS_MISDB.[CurDat].[DerivativeDetail] MAIN ON TEMP.DerivativeRefNo=MAIN.DerivativeRefNo
WHERE MAIN.EffectiveToTimeKey=49999
 
--GO
/*********************************************************************************************************/
/*  New Customers Customer Entity ID Update  */
DECLARE @DerivativeEntityId INT=0 
SELECT @DerivativeEntityId=MAX(DerivativeEntityId) FROM  UTKS_MISDB.[curdat].[DerivativeDetail]
IF @DerivativeEntityId IS NULL  
BEGIN
SET @DerivativeEntityId=0
END
 
UPDATE TEMP 
SET TEMP.DerivativeEntityID=ACCT.DerivativeEntityId
 FROM UTKS_TEMPDB.DBO.[TEMPDERVCANCEL_SOURCESYSTEM] TEMP
INNER JOIN (SELECT DerivativeRefNo,(@DerivativeEntityId + ROW_NUMBER()OVER(ORDER BY (SELECT 1))) DerivativeEntityId
			FROM UTKS_TEMPDB.DBO.[TEMPDERVCANCEL_SOURCESYSTEM]
			WHERE DerivativeEntityID=0 OR DerivativeEntityID IS NULL)ACCT ON TEMP.DerivativeRefNo=ACCT.DerivativeRefNo

--------------------------------------------------


end



GO