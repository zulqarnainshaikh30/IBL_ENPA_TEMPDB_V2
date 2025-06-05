SET QUOTED_IDENTIFIER, ANSI_NULLS ON
GO

-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [ETL_TEMP].[AdvFacPCDetail_Temp] 
	-- Add the parameters for the stored procedure here
	
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

    -- Insert statements for procedure here
	
DECLARE @vEFFECTIVEFROM SMALLINT = (SELECT Timekey FROM IBL_ENPA_DB_V2.dbo.Automate_Advances WHERE EXT_FLG = 'Y')
----TRUNCATE TABLE [MISDB_31052018].[CURDAT].[AdvFacPCDetail]
TRUNCATE TABLE [TempAdvFacPCDetail]--[dbo].[TempAdvFacPCDetail]

INSERT INTO [TempAdvFacPCDetail]
           (
AccountEntityId
,PCRefNo
,PCAdvDt
,PCAmt
,PCDueDt
,PCDurationDays
,PCExtendedDueDt
,ExtensionReason
,CurrencyAlt_Key
,LcNo
,LcAmt
,LcIssueDt
,LcIssuingBank_FirmOrder
,Balance
,BalanceInCurrency
,BalanceInUSD
,Overdue
,CommodityAlt_Key
,CommodityValue
,CommodityMarketValue
,ShipmentDt
,CommercialisationDt
,EcgcPolicyNo
,CAD
,CADU
,OverDueSinceDt
,TotalProv
,Secured
,Unsecured
,Provsecured
,ProvUnsecured
,ProvDicgc
,npadt
,CoverGovGur
,DerecognisedInterest1
,DerecognisedInterest2
,ClaimType
,ClaimCoverAmt
,ClaimLodgedDt
,ClaimLodgedAmt
,ClaimRecvDt
,ClaimReceivedAmt
,ClaimRate
,AdjDt
,EntityClosureDate
,EntityClosureReasonAlt_Key
,RefSystemAcid
,AuthorisationStatus
,EffectiveFromTimeKey
,EffectiveToTimeKey
,CreatedBy
,DateCreated
,ModifiedBy
,DateModified
,ApprovedBy
,DateApproved
,UnAppliedIntt
,MocTypeAlt_Key
,MocStatus
,MocDate
,RBI_ExtnPermRefNo
,LC_OrderAlt_Key
,OrderLC_CurrencyAlt_Key
,CountryAlt_Key
,LcAmtInCurrenc
,PcEntityId
,Ischanged 
) 

SELECT DISTINCT 
AccountID	AccountEntityId
,PCRefNo	PCRefNo
,PCAdvDt	PCAdvDt
,PCAmt	PCAmt
,PCDueDt	PCDueDt
,PCDurationDays	PCDurationDays
,PCExtendedDueDt	PCExtendedDueDt
,ExtensionReason	ExtensionReason
,CC.CurrencyAlt_Key	CurrencyAlt_Key
,LcNo	LcNo
,LcAmt	LcAmt
,LcIssueDt	LcIssueDt
,LcIssuingBank_FirmOrder	LcIssuingBank_FirmOrder
,BalanceInINR	Balance
,BalanceInCurrency	BalanceInCurrency
,BalanceInUSD	BalanceInUSD
,OverdueAmt	Overdue
,null	CommodityAlt_Key
,null	CommodityValue
,null	CommodityMarketValue
,null	ShipmentDt
,null	CommercialisationDt
,null	EcgcPolicyNo
,null	CAD
,null	CADU
,null	OverDueSinceDt
,null	TotalProv
,null	Secured
,null	Unsecured
,null	Provsecured
,null	ProvUnsecured
,null	ProvDicgc
,Npadt	npadt
,null	CoverGovGur
,null	DerecognisedInterest1
,null	DerecognisedInterest2
,null	ClaimType
,null	ClaimCoverAmt
,null	ClaimLodgedDt
,null	ClaimLodgedAmt
,null	ClaimRecvDt
,null	ClaimReceivedAmt
,null	ClaimRate
,null	AdjDt
,EntityClosureDate	EntityClosureDate
,0 --EntityClosureReasonCode	EntityClosureReasonAlt_Key
,RefSystemAcid	RefSystemAcid
,null	AuthorisationStatus
,@vEFFECTIVEFROM	EffectiveFromTimeKey
,49999	EffectiveToTimeKey
,'SSISUSER' CreatedBy
,GETDATE() DateCreated
,null	ModifiedBy
,null	DateModified
,null	ApprovedBy
,null	DateApproved
,UnAppliedIntt	UnAppliedIntt
,null	MocTypeAlt_Key
,null	MocStatus
,null	MocDate
,null	RBI_ExtnPermRefNo
,null	LC_OrderAlt_Key
,null	OrderLC_CurrencyAlt_Key
,null	CountryAlt_Key
,null	LcAmtInCurrenc
,null	PcEntityId
,null	Ischanged 
FROM  TempADVACBASICDETAIL AA INNER JOIN IBL_ENPA_STGDB_V2.dbo.PC_SOURCESYSTEM_STG PC 
		ON PC.AccountID=AA.CustomerAcid
	LEFT JOIN IBL_ENPA_DB_V2.dbo.DimCurrency cc
		on cc.CurrencyCode=PC.CurrencyCode  
END


GO