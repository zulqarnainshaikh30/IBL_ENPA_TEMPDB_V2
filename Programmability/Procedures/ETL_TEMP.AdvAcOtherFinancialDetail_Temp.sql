SET QUOTED_IDENTIFIER, ANSI_NULLS ON
GO


-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [ETL_TEMP].[AdvAcOtherFinancialDetail_Temp] 
	-- Add the parameters for the stored procedure here
	
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

    -- Insert statements for procedure here
	
TRUNCATE TABLE dbo.[TempAdvAcOtherFinancialDetail]

DECLARE  @vEffectivefrom  Int SET @vEffectiveFrom=(SELECT TimeKey FROM IBL_ENPA_DB_V2.[dbo].SYSDATAMATRIX WHERE CurrentStatus='C')
DECLARE @TimeKey  Int SET @TimeKey=(SELECT TimeKey FROM IBL_ENPA_DB_V2.[dbo].SYSDATAMATRIX WHERE CurrentStatus='C')
DECLARE @DATE AS DATE =(SELECT Date FROM IBL_ENPA_DB_V2.[dbo].SYSDATAMATRIX WHERE CurrentStatus='C')
--------------------------------------------------------------------------------------------------------------------------------------------------



INSERT INTO dbo.[TempAdvAcOtherFinancialDetail]
(	            
AccountEntityId
,RefSystemAcId
,int_receivable_adv
,penal_int_receivable
,Accrued_interest
,penal_due
,Interest_due
,other_dues
,Overdueinterest
--,PenalOverdueinterest
,UnAppliedIntAmount
--,PenalUnAppliedIntAmount
--,PenalInterestOverDueSInceDt
,AuthorisationStatus
,EffectiveFromTimeKey
,EffectiveToTimeKey
,CreatedBy
,DateCreated
,ModifiedBy
,DateModified
,ApprovedBy
,DateApproved
--,D2Ktimestamp

)
--------------------------------LMS-----------------------------------

	SELECT 
			ACBD.AccountEntityId AS  AccountEntityId			
			,ACBD.SystemACID [RefSystemAcId]
			,NULL int_receivable
			,NULL penal_int_receivable
			,NULL Accured_Interest
			,penal_due
			,Interest_due
			,NULL other_dues
			,Overdueinterest
			--,PenalInterestOverdueAmt
			,UnAppliedIntAmount
			--,PenalUnAppliedInterestAmt
			--,PenalInterestOverDueSInceDt
			,NULL AS AuthorisationStatus
			,@vEffectivefrom AS EffectiveFromTimeKey
			,49999 AS EffectiveToTimeKey
			,'SSISUSER' AS CreatedBy
			,GETDATE() AS DateCreated
			,NULL AS ModifiedBy
			,NULL AS DateModified
			,NULL AS ApprovedBy
			,NULL AS DateApproved
			--,GETDATE() AS D2Ktimestamp			
			FROM IBL_ENPA_STGDB_V2.DBO.ACCOUNT_ALL_SOURCE_SYSTEM A
			left JOIN IBL_ENPA_DB_V2.DBO.DIMSOURCEDB DS ON DS.SourceName=A.SourceSystem
			AND DS.EffectiveFromTimeKey<=@TimeKey And DS.EffectiveToTimeKey>=@TimeKey
			INNER JOIN dbo.TempAdvAcBasicDetail ACBD ON A.CustomerAcID=ACBD.CustomerACID ---And DS.SourceAlt_Key=ACBD.SourceAlt_Key
			LEFT JOIN dbo.TempAdVAcBalanceDetail AABD  on AABD.AccountEntityId=ACBD.AccountEntityId
			INNER JOIN IBL_ENPA_DB_V2.DBO.DIMPRODUCT P ON P.EffectiveFromTimeKey<=@TIMEKEY AND P.EFFECTIVETOTIMEKEY>=@TIMEKEY 
			AND ACBD.PRODUCTALT_KEY=P.PRODUCTALT_KEY
/*
UNION

--------------------------------INDUS-----------------------------------

	SELECT 
			ACBD.AccountEntityId AS  AccountEntityId
			,NULL AS Ac_LastReviewDueDt
			,0 AS Ac_ReviewTypeAlt_key
			,NULL AS Ac_ReviewDt
			,0 AS Ac_ReviewAuthAlt_Key
			,NULL AS Ac_NextReviewDueDt
			,NULL AS DrawingPower   --WDLMT  lnddalyt
			,A.InttRate AS InttRate
			--,NULL AS IrregularType
			--,NULL AS IrregularityDt
			,A.NPADate AS NpaDt
			,NULL AS BookDebts
			,NULL AS UnDrawnAmt
			--,NULL AS TotalDI
			--,NULL AS UnAppliedIntt
			--,NULL AS LegalExp
			,NULL AS UnAdjSubSidy
			,NULL AS LastInttRealiseDt
			,NULL AS MocStatus
			,NULL AS MOCReason
			--,NULL AS WriteOffAmt_HO   --LoanStatusID  l_loan for write off indification  n for npa
			--,NULL AS InterestRateCodeAlt_Key
			--,NULL AS WriteOffDt
			--,NULL AS OD_Dt
			,NULL AS LimitDisbursed---  DisbursedAmount  l_loan     tdr for lndality
			--,NULL AS WriteOffAmt_BR
			,ACBD.RefCustomerId [RefCustomerId]
			,ACBD.SystemACID [RefSystemAcId]
			,NULL AS AuthorisationStatus
			,@vEffectivefrom AS EffectiveFromTimeKey
			,49999 AS EffectiveToTimeKey
			,'SSISUSER' AS CreatedBy
			,GETDATE() AS DateCreated
			,NULL AS ModifiedBy
			,NULL AS DateModified
			,NULL AS ApprovedBy
			,NULL AS DateApproved
			,NULL AS D2Ktimestamp
			,NULL AS MocDate
			,NULL AS MocTypeAlt_Key
			,NULL AS CropDuration
			,NULL AS Ac_ReviewAuthLevelAlt_Key
			FROM IBL_ENPA_STGDB_V2.DBO.ACCOUNT_SOURCESYSTEM02_STG A
			INNER JOIN IBL_ENPA_DB_V2.DBO.DIMSOURCEDB DS ON DS.SourceName=A.SourceSystem
			AND DS.EffectiveFromTimeKey<=@TimeKey And DS.EffectiveToTimeKey>=@TimeKey
			INNER JOIN TempAdvAcBasicDetail ACBD ON A.CustomerAcID=ACBD.CustomerACID And DS.SourceAlt_Key=ACBD.SourceAlt_Key
			LEFT JOIN TempAdVAcBalanceDetail AABD  on AABD.AccountEntityId=ACBD.AccountEntityId


*/
END




GO