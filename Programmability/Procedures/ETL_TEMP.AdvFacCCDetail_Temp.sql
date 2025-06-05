SET QUOTED_IDENTIFIER, ANSI_NULLS ON
GO

-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [ETL_TEMP].[AdvFacCCDetail_Temp] 
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

TRUNCATE TABLE dbo.[TempAdvFacCCDetail]
--select * from dbo.[TempAdvFacCCDetail]
INSERT INTO dbo.[TempAdvFacCCDetail]
           ([AccountEntityId]
           ,[AdhocDt]
           ,[AdhocAmt]
           ,[ContExcsSinceDt]
           --,[MarginAmt]
           ,[DerecognisedInterest1]
           ,[DerecognisedInterest2]
           --,[AdjReasonAlt_Key]
           --,[EntityClosureDate]
           --,[EntityClosureReasonAlt_Key]
           ,[ClaimType]
           ,[ClaimCoverAmt]
           ,[ClaimLodgedDt]
           ,[ClaimLodgedAmt]
           ,[ClaimRecvDt]
           ,[ClaimReceivedAmt]
           ,[ClaimRate]
           ,[RefSystemAcid]
           ,[AuthorisationStatus]
           ,[EffectiveFromTimeKey]
           ,[EffectiveToTimeKey]
           ,[CreatedBy]
           ,[DateCreated]
           ,[ModifiedBy]
           ,[DateModified]
           ,[ApprovedBy]
           ,[DateApproved]
           ,[MocStatus]
           ,[MocDate]
           ,[MocTypeAlt_Key]
           ,[AdhocExpiryDate]
           --,[AdhocPermittedAlt_key]
           --,[AdhocAuth_ID]
           --,[AdhocNormalInterest]
		   ,StockStmtDt
		   )

------------------LMS------------

SELECT 
            ACBD.AccountEntityId AS [AccountEntityId]
           ,A.AdhocDate [AdhocDt]
           ,A.AdhocAmt [AdhocAmt]
           ,a.ContiExcessDate [ContExcsSinceDt]
           --,NULL [MarginAmt]
           ,NULL [DerecognisedInterest1]
           ,NULL [DerecognisedInterest2]
           --,0 AS [AdjReasonAlt_Key]
           --,NULL [EntityClosureDate]
           --,0 AS [EntityClosureReasonAlt_Key]
           ,NULL [ClaimType]
           ,NULL [ClaimCoverAmt]
           ,NULL [ClaimLodgedDt]
           ,NULL [ClaimLodgedAmt]
           ,NULL [ClaimRecvDt]
           ,NULL [ClaimReceivedAmt]
           ,NULL [ClaimRate]
           ,ACBD.SystemAcId AS [RefSystemAcid]
           ,NULL [AuthorisationStatus]
           ,@vEffectivefrom AS [EffectiveFromTimeKey]
           ,49999 AS [EffectiveToTimeKey]
           ,'SSISUSER' AS [CreatedBy]
           ,GETDATE() AS [DateCreated]
           ,NULL [ModifiedBy]
           ,NULL [DateModified]
           ,NULL [ApprovedBy]
           ,NULL [DateApproved]
           ,NULL [MocStatus]
           ,NULL [MocDate]
           ,NULL [MocTypeAlt_Key]
           ,NULL [AdhocExpiryDate]
           --,NULL [AdhocPermittedAlt_key]
           --,NULL [AdhocAuth_ID]
           --,NULL [AdhocNormalInterest]
		   ,A.StockStatementDt
FROM dbo.TempAdvAcBasicDetail ACBD
INNER JOIN IBL_ENPA_STGDB_V2.dbo.ACCOUNT_ALL_SOURCE_SYSTEM A ON A.customeracid=ACBD.CustomerAcid
LEFT JOIN IBL_ENPA_DB_V2.DBO.DimGLProduct DGP ON DGP.GLProductAlt_Key=ACBD.GLProductAlt_Key and DGP.EffectiveFromTimeKey<=@TimeKey AND DGP.EffectiveToTimeKey>=@TimeKey
WHERE ACBD.FacilityType IN ('OD' ,'CC','CCOD')
/*
UNION 

-------------Other----------------

SELECT 
            ACBD.AccountEntityId AS [AccountEntityId]
           ,A.AdhocDate [AdhocDt]
           ,A.AdhocAmt [AdhocAmt]
           ,NULL [ContExcsSinceDt]
           --,NULL [MarginAmt]
           ,NULL [DerecognisedInterest1]
           ,NULL [DerecognisedInterest2]
           --,0 AS [AdjReasonAlt_Key]
           --,NULL [EntityClosureDate]
           --,0 AS [EntityClosureReasonAlt_Key]
           ,NULL [ClaimType]
           ,NULL [ClaimCoverAmt]
           ,NULL [ClaimLodgedDt]
           ,NULL [ClaimLodgedAmt]
           ,NULL [ClaimRecvDt]
           ,NULL [ClaimReceivedAmt]
           ,NULL [ClaimRate]
           ,ACBD.SystemAcId AS [RefSystemAcid]
           ,NULL [AuthorisationStatus]
           ,@vEffectivefrom AS [EffectiveFromTimeKey]
           ,49999 AS [EffectiveToTimeKey]
           ,'SSISUSER' AS [CreatedBy]
           ,GETDATE() AS [DateCreated]
           ,NULL [ModifiedBy]
           ,NULL [DateModified]
           ,NULL [ApprovedBy]
           ,NULL [DateApproved]
           ,NULL [MocStatus]
           ,NULL [MocDate]
           ,NULL [MocTypeAlt_Key]
           ,NULL [AdhocExpiryDate]
           --,NULL [AdhocPermittedAlt_key]
           --,NULL [AdhocAuth_ID]
           --,NULL [AdhocNormalInterest]

FROM TempAdvAcBasicDetail ACBD
INNER JOIN IBL_ENPA_STGDB_V2.dbo.ACCOUNT_SOURCESYSTEM02_STG A ON A.customeracid=ACBD.CustomerAcid
LEFT JOIN IBL_ENPA_DB_V2.DBO.DimGLProduct DGP ON DGP.GLProductAlt_Key=ACBD.GLProductAlt_Key and DGP.EffectiveFromTimeKey<=@TimeKey AND DGP.EffectiveToTimeKey>=@TimeKey
WHERE ACBD.FacilityType IN ('OD' ,'CC','CCOD')




*/
END

---Remove Duplicate----(Vinit Tamgadge)
;with cta as
(select ROW_NUMBER() over (partition by AccountEntityId order by AccountEntityId) ACID,
* from TempAdvFacCCDetail)  
delete from Cta  where ACID>1


GO