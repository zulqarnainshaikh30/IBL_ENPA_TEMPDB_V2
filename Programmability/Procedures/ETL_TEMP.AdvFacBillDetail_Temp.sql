SET QUOTED_IDENTIFIER, ANSI_NULLS ON
GO




-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [ETL_TEMP].[AdvFacBillDetail_Temp] 
	-- Add the parameters for the stored procedure here
	
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

    -- Insert statements for procedure here
	
TRUNCATE TABLE dbo.[TempAdvFacBillDetail]

DECLARE  @vEffectivefrom  Int SET @vEffectiveFrom=(SELECT TimeKey FROM UTKS_MISDB.[dbo].Automate_Advances WHERE EXT_FLG='Y')
DECLARE @TimeKey  Int SET @TimeKey=(SELECT TimeKey FROM UTKS_MISDB.[dbo].Automate_Advances WHERE EXT_FLG='Y')
DECLARE @DATE AS DATE =(SELECT Date FROM UTKS_MISDB.[dbo].Automate_Advances WHERE EXT_FLG='Y')


/*************************************************************************************************************************/ 

INSERT INTO dbo.[TempAdvFacBillDetail]
           (
				AccountEntityId
				,BillEntityId
				,BillNo
				,BillDt
				,BillAmt
				,BillRefNo
				,BillPurDt
				,AdvAmount
				,BillDueDt
				,BillExtendedDueDt
				,CrystalisationDt
				,CommercialisationDt
				,BillNatureAlt_Key
				,BillAcceptanceDt
				,UsanceDays
				,DraweeNo
				,DraweeBankName
				,DrawerName
				,PayeeName
				,CollectingBankName
				,CollectingBranchPlace
				,InterestIncome
				,Commission
				,DiscountCharges
				,DelayedInt
				,MarginType
				,MarginAmt
				,CountryAlt_Key
				,BillOsReasonAlt_Key
				,CommodityAlt_Key
				,LcNo
				,LcAmt
				,LcIssuingBankAlt_Key
				,LcIssuingBank
				,CurrencyAlt_Key
				,Balance
				,BalanceInCurrency
				,Overdue
				,DerecognisedInterest1
				,DerecognisedInterest2
				,OverDueSinceDt
				,TotalProv
				,AdditionalProv
				,GenericAddlProv
				,Secured
				,CoverGovGur
				,Unsecured
				,Provsecured
				,ProvUnsecured
				,ProvDicgc
				,npadt
				,ClaimType
				,ClaimCoverAmt
				,ClaimLodgedDt
				,ClaimLodgedAmt
				,ClaimRecvDt
				,ClaimReceivedAmt
				,ClaimRate
				,ScrCrError
				,RefSystemAcid
				,AdjDt
				,AdjReasonAlt_Key
				,EntityClosureDate
				,EntityClosureReasonAlt_Key
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
				,MocTypeAlt_Key
				,ScrCrErrorSeq
				,ConsigmentExport
			)
SELECT
			A.AccountEntityId		AccountEntityId
			,0						BillEntityId
			,A.AccountEntityId		BillNo
			,NULL					BillDt
			,case when isnull(BILL.BalanceOutstandingINR,0)<=0 then isnull(BILL.BalanceOutstandingINR,0)*-1 else 0 end AS	BillAmt
			,A.AccountEntityId			BillRefNo
			,NULL			BillPurDt
			,case when isnull(BILL.BalanceOutstandingINR,0)<=0 then isnull(BILL.BalanceOutstandingINR,0)*-1 else 0 end AS		AdvAmount
			,NULL			BillDueDt
			,NULL					BillExtendedDueDt
			,NULL					CrystalisationDt
			,NULL	CommercialisationDt
			,NULL		BillNatureAlt_Key
			,NULL				BillAcceptanceDt
			,NULL				UsanceDays
			,NULL				DraweeNo
			,NULL				DraweeBankName
			,NULL				DrawerName
			,NULL				PayeeName
			,NULL				CollectingBankName
			,NULL				CollectingBranchPlace
			,NULL				InterestIncome
			,NULL				Commission
			,NULL				DiscountCharges
			,NULL				DelayedInt
			,NULL				MarginType
			,NULL				MarginAmt
			,NULL				CountryAlt_Key
			,NULL				BillOsReasonAlt_Key
			,NULL				CommodityAlt_Key
			,NULL				LcNo
			,NULL				LcAmt
			,NULL				LcIssuingBankAlt_Key
			,NULL		LcIssuingBank
			,C.CurrencyAlt_Key		CurrencyAlt_Key
			,case when isnull(BILL.BalanceOutstandingINR,0)<=0 then isnull(BILL.BalanceOutstandingINR,0)*-1 else 0 end AS		Balance
			,BILL.BalanceInActualAcCurrency	BalanceInCurrency
			,NULL				Overdue
			,NULL				DerecognisedInterest1
			,NULL				DerecognisedInterest2
			,NULL					OverDueSinceDt
			,NULL					TotalProv
			,NULL					AdditionalProv
			,NULL					GenericAddlProv
			,NULL					Secured
			,NULL					CoverGovGur
			,NULL					Unsecured
			,NULL					Provsecured
			,NULL					ProvUnsecured
			,NULL					ProvDicgc
			,BILL.NPADate					npadt
			,NULL					ClaimType
			,NULL					ClaimCoverAmt
			,NULL					ClaimLodgedDt
			,NULL					ClaimLodgedAmt
			,NULL					ClaimRecvDt
			,NULL					ClaimReceivedAmt
			,NULL					ClaimRate
			,NULL					ScrCrError
			,A.customeracid			RefSystemAcid
			,A.AdjDt				AdjDt
			,A.AdjReasonAlt_Key		AdjReasonAlt_Key
			,NULL					EntityClosureDate
			,NULL					EntityClosureReasonAlt_Key
			,NULL					AuthorisationStatus
			,@TimeKey				EffectiveFromTimeKey
			,49999					EffectiveToTimeKey0
			,'SSISUSER'				CreatedBy
			,GETDATE()				DateCreated
			,NULL					ModifiedBy
			,NULL					DateModified
			,NULL					ApprovedBy
			,NULL					DateApproved
			,NULL					D2Ktimestamp
			,NULL					MocStatus
			,NULL					MocDate
			,NULL					MocTypeAlt_Key
			,NULL					ScrCrErrorSeq
			,NULL					ConsigmentExport
	FROM							UTKS_STGDB.[dbo].[LMS_ACCOUNT_STG] BILL
	INNER JOIN						dbo.TempAdvAcBasicDetail A ON A.customeracid=BILL.customeracid
	LEFT JOIN						UTKS_MISDB.DBO.DimCurrency C ON BILL.CurrencyCode=C.CurrencyCode
	AND								C.EffectiveFromTimeKey<=@TimeKey AND C.EffectiveToTimeKey>=@TimeKey
	 WHERE							A.FacilityType IN('BD','BP','BILL')


	 UPDATE TEMP 
SET TEMP.BillEntityId=MAIN.BillEntityId
 
FROM  UTKS_TEMPDB.DBO.[TempAdvFacBillDetail] TEMP
INNER JOIN UTKS_MISDB.[DBO].[AdvFacBillDetail] MAIN ON TEMP.AccountEntityId=MAIN.AccountEntityId
			AND TEMP.BILLNO=MAIN.BILLNO
			AND TEMP.BILLREFNO=MAIN.BILLREFNO
WHERE MAIN.EffectiveToTimeKey=49999
--GO
/*********************************************************************************************************/
/*  New Customers Account Entity ID Update  */
DECLARE @BillEntityId INT=0 
SELECT @BillEntityId=MAX(BillEntityId) FROM  UTKS_MISDB.[dbo].[AdvFacBillDetail] 
IF @BillEntityId IS NULL  
BEGIN
SET @BillEntityId=0
END
 
UPDATE TEMP 
SET TEMP.BillEntityId=ACCT.BillEntityId
 FROM UTKS_TEMPDB.DBO.[TempAdvFacBillDetail] TEMP
INNER JOIN (SELECT AccountEntityId,BILLNO,BillRefNo,(@BillEntityId + ROW_NUMBER()OVER(ORDER BY (SELECT 1))) BillEntityId
			FROM UTKS_TEMPDB.DBO.[TempAdvFacBillDetail]
			WHERE BillEntityId=0 OR BillEntityId IS NULL)
		ACCT 
		ON TEMP.AccountEntityId=ACCT.AccountEntityId
		AND TEMP.BILLNO		=ACCT.BILLNO		
		AND TEMP.BillRefNo	=ACCT.BillRefNo

update a set BillDueDt=b.AcOpenDt
from TempAdvFacBillDetail a
inner join UTKS_STGDB.[dbo].[LMS_ACCOUNT_STG]b
on a.RefSystemAcid=b.customeracid
where b.Scheme_ProductCode='236'

END


GO