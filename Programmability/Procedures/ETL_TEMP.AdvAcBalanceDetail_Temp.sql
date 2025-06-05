SET QUOTED_IDENTIFIER, ANSI_NULLS ON
GO
 
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [ETL_TEMP].[AdvAcBalanceDetail_Temp] 
	-- Add the parameters for the stored procedure here
	
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	BEGIN TRY
	SET NOCOUNT ON;

    -- Insert statements for procedure here 


TRUNCATE TABLE [dbo].TEMPADVACBALANCEDETAIL

DECLARE  @vEffectivefrom  Int SET @vEffectiveFrom=(SELECT TimeKey FROM IBL_ENPA_DB_V2.[dbo].SYSDATAMATRIX WHERE CurrentStatus='C')
DECLARE @TimeKey  Int SET @TimeKey=(SELECT TimeKey FROM IBL_ENPA_DB_V2.[dbo].SYSDATAMATRIX WHERE CurrentStatus='C')
DECLARE @DATE AS DATE =(SELECT Date FROM IBL_ENPA_DB_V2.[dbo].SYSDATAMATRIX WHERE CurrentStatus='C')

----------------------------------------------------------------------------------------------------------------------------------------------------
/* For All Accounts */ 
--select * from [TEMPADVACBALANCEDETAIL]
SET IDENTITY_INSERT TEMPADVACBALANCEDETAIL OFF

set dateformat dmy
INSERT INTO [dbo].[TEMPADVACBALANCEDETAIL]
(

--		 ACCOUNTENTITYID,
		ASSETCLASSALT_KEY
		,BALANCEINCURRENCY
		,BALANCE
		,SIGNBALANCE
		,LASTCRDT
		,OVERDUE
		,TOTALPROV
		--,DIRECTBALANCE
		--,INDIRECTBALANCE
		--,LASTCRAMT
		,REFCUSTOMERID
		,REFSYSTEMACID
		,AUTHORISATIONSTATUS
		,EFFECTIVEFROMTIMEKEY
		,EFFECTIVETOTIMEKEY
		,OVERDUESINCEDT
		,MOCSTATUS
		,MOCDATE
		,MOCTYPEALT_KEY
		,OLD_OVERDUESINCEDT
		,OLD_OVERDUE
		,ORG_TOTALPROV
		,INTREVERSEAMT
		,PS_BALANCE
		,NPS_BALANCE
		,DATECREATED
		,MODIFIEDBY
		,DATEMODIFIED
		,APPROVEDBY
		,DATEAPPROVED
		,CREATEDBY
		,PS_NPS_FLAG
		,UPGRADEDATE
		,OverduePrincipal
		,OverduePrincipalDt
		,Overdueinterest
		,OverdueIntDt
		,OverOtherdue
		,OverdueOtherDt
		,UnAppliedIntAmount
		,PrincipalBalance
		,SourceAssetClass
		,SourceNpaDate
		--,DPD_Bank
		)

		-----------------------LMS DATA-----------------------
		SELECT 

--		ACBD.ACCOUNTENTITYID,
		1--DA.AssetClassAlt_Key AS ASSETCLASSALT_KEY
		,ACC1.BalanceInActualAcCurrency AS BALANCEINCURRENCY
		,(CASE WHEN aCC1.BalanceOutstandingINR <= 0 THEN 0 ELSE aCC1.BalanceOutstandingINR END)BalanceOutstandingINR--case when isnull(ACC1.BalanceOutstandingINR,0)<=0 then isnull(ACC1.BalanceOutstandingINR,0)*-1 else 0 end AS BALANCE
		,ACC1.BalanceOutstandingINR AS SIGNBALANCE
		,CASE WHEN ACBD.ReferencePeriod in(365,366,180,181) then null ELSE  ACC1.LastCreditDate  END LASTCRDT
		,isnull(PrincipalOverdueAmt,0)+isnull(InterestOverdueAmt,0)+isnull(OthChargesOverdueAmt,0) AS OVERDUE  --TDUE
		,NULL AS TOTALPROV
		--,NULL AS DIRECTBALANCE
		--,NULL AS INDIRECTBALANCE
		--,NULL AS LASTCRAMT
		,ACBD.REFCUSTOMERID [REFCUSTOMERID]
		,ACBD.SYSTEMACID [REFSYSTEMACID]
		,ACBD.AuthorisationStatus
		,@VEFFECTIVEFROM [EFFECTIVEFROMTIMEKEY]
		,49999 [EFFECTIVETOTIMEKEY]
		,	IBL_ENPA_DB_V2.PRO.GETMINIMUMDATE(PrincipalOverDueSinceDt,InterestOverDueSinceDt,OthChangesOverDueSinceDt)--,PenalInterestOverDueSinceDt)
			as OVERDUESINCEDT  --GET MINIMUM DATE FOR OVERDUE SINCE DATE
		--,CASE WHEN DAYSDELQ >0  THEN CASE WHEN DIST1ND='NULL'  OR  DIST1ND='' THEN NULL ELSE CONVERT(Date, DIST1ND, 103) END END AS  OVERDUESINCEDT -- AS PER DICUSIION ON 12/06/2019
		,'N' AS MOCSTATUS
		,NULL AS MOCDATE
		,NULL AS MOCTYPEALT_KEY
		,NULL AS OLD_OVERDUESINCEDT
		,NULL AS OLD_OVERDUE
		,NULL AS ORG_TOTALPROV
		,null AS INTREVERSEAMT -- amar added  10062021
		,0 AS PS_BALANCE
		,0 AS NPS_BALANCE
		,GETDATE() AS DATECREATED
		,NULL AS MODIFIEDBY
		,NULL AS DATEMODIFIED
		,NULL AS APPROVEDBY
		,NULL AS DATEAPPROVED
		,'SSISUSER' AS CREATEDBY
		,NULL AS PS_NPS_FLAG
		,NULL AS UPGRADEDATE
		,ISNULL(ACC1.PrincipalOverdueAmt,0)  AS OverduePrincipal  --overdue principal 
		,PrincipalOverDueSinceDt AS OverduePrincipaldate  --overdue principal date
		,ISNULL(ACC1.InterestOverdueAmt,0) AS Overdueintereset  --overdue intereset  
		, ACC1.InterestOverDueSinceDt AS Overdueinteresetdate--overdue intereset  
		,acc1.OthChargesOverdueAmt
		,acc1.OthChangesOverDueSinceDt
		,acc1.UnAppliedInterestAmt
		,isnull(ACC1.POSBalance,0) AS POSBalance
		,acc1.AssetClassCode SourceAssetClass
		,acc1.NPADate		 SourceNpaDate
		--,DPD_Bank
	FROM IBL_ENPA_STGDB_V2.DBO.ACCOUNT_ALL_SOURCE_SYSTEM ACC1
		LEFT JOIN IBL_ENPA_TEMPDB_V2.DBO.TempAdvAcBasicDetail ACBD ON  ACC1.CustomerAcID=ACBD.CustomerACID
		LEFT JOIN IBL_ENPA_DB_V2.DBO.DimAssetClassMapping DA ON ACC1.AssetClassCode=DA.SrcSysClassCode
		AND DA.EffectiveFromTimeKey<=@TimeKey AND DA.EffectiveToTimeKey>=@TimeKey

	

END TRY
BEGIN CATCH

Update IBL_ENPA_STGDB_V2.[dbo].[PACKAGE_AUDIT] 
SET ExecutionStatus = -1 
where Tablename = 'Temp_AdvAcBalanceDetail' 
and PackageName = 'StageToTempDB' 
and Execution_Date = cast(GETDATE() as date)

Update IBL_ENPA_DB_V2.[dbo].[BANDAUDITSTATUS]
set BandStatus = 'Failed' 
where BandName in ('StageToTemp')

RAISERROR('Insert Failed',16,2)
END CATCH
END




GO