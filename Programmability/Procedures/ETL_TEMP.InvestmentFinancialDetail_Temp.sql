SET QUOTED_IDENTIFIER, ANSI_NULLS ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [ETL_TEMP].[InvestmentFinancialDetail_Temp] 
	-- Add the parameters for the stored procedure here
	
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

    -- Insert statements for procedure here
	

DECLARE  @vEffectivefrom  Int SET @vEffectiveFrom=(SELECT TimeKey FROM IBL_ENPA_DB_V2.[dbo].Automate_Advances WHERE EXT_FLG='Y')
DECLARE @TimeKey  Int SET @TimeKey=(SELECT TimeKey FROM IBL_ENPA_DB_V2.[dbo].Automate_Advances WHERE EXT_FLG='Y')
DECLARE @DATE AS DATE =(SELECT Date FROM IBL_ENPA_DB_V2.[dbo].Automate_Advances WHERE EXT_FLG='Y')

TRUNCATE TABLE TempInvestmentFinancialDetail

INSERT INTO TempInvestmentFinancialDetail
			(Time_Key
			,Inv_Key
			,HoldingNature
			,CurrencyAlt_Key
			,CurrencyConvRate			
			,BookValue
			,BookValueINR
			,MTMValue
			,MTMValueINR
			,EncumberedMTM
			,AssetClass_AltKey
			,NPIDt
			,TotalProvison
			,AuthorisationStatus
			,EffectiveFromTimeKey
			,EffectiveToTimeKey
			,CreatedBy
			,DateCreated
			,ModifiedBy
			,DateModified
			,ApprovedBy
			,DateApproved
			,DBTDate
			,LatestBSDate
			,Interest_DividendDueDate
			,Interest_DividendDueAmount
			,PartialRedumptionDueDate
			,PartialRedumptionSettledY_N
			,FLGDEG
			,DEGREASON
			,DPD
			,FLGUPG
			,UpgDate
			,PROVISIONALT_KEY
			,InitialAssetAlt_Key
			,InitialNPIDt
			,RefInvId
			,REFISSUERID
			,ISIN
			)

SELECT		@TimeKey Time_Key
			,NULL Inv_Key
			,HoldingNature
			,C.CurrencyAlt_Key
			,CurrencyConvRate	
			,BookValue
			,BookValueINR
			,MTMValue
			,MTMValueINR
			,NULL EncumberedMTM
			,B.AssetClassAlt_Key
			,A.NPIDt
			,NULL TotalProvison
			,NULL AuthorisationStatus
			,@vEffectivefrom EffectiveFromTimeKey
			,49999 EffectiveToTimeKey
			,'SSISUSER' CreatedBy
			,GETDATE() DateCreated
			,NULL ModifiedBy
			,NULL DateModified
			,NULL ApprovedBy
			,NULL DateApproved
			,NULL DBTDate
			,A.LatestBSDate LatestBSDate
			,A.Interest_DividendOverdueDate Interest_DividendDueDate
			,A.Interest_DividendOverdueAmount Interest_DividendDueAmount
			,A.RedumptionDueDate PartialRedumptionDueDate
			,A.RedumptionProceedSettledY_N PartialRedumptionSettledY_N
			,NULL FLGDEG
			,NULL DEGREASON
			,NULL DPD
			,NULL FLGUPG
			,NULL UpgDate
			,NULL PROVISIONALT_KEY
			,B.AssetClassAlt_Key InitialAssetAlt_Key
			,A.NPIDt InitialNPIDt
			,A.InvId
			,A.IssuerID
			,A.ISIN
FROM IBL_ENPA_STGDB_V2.DBO.TREASURY_INVFINANCIAL_STG  A -- TREASURY
	LEFT JOIN IBL_ENPA_DB_V2.DBO.DimAssetClass B ON A.AssetClass=B.SrcSysClassCode
	AND B.EffectiveFromTimeKey<=@TimeKey AND B.EffectiveToTimeKey>=@TimeKey
	LEFT JOIN IBL_ENPA_DB_V2.DBO.DimCurrency C ON A.CurrencyCode=C.SrcSysCurrencyCode
	AND C.EffectiveFromTimeKey<=@TimeKey AND C.EffectiveToTimeKey>=@TimeKey

Update A set A.InvEntityId=B.InvEntityId
from IBL_ENPA_TEMPDB_V2.DBO.TempInvestmentFinancialDetail A
Inner Join IBL_ENPA_TEMPDB_V2.DBO.TempInvestmentBasicDetail B ON A.RefInvID=B.InvID


END


GO