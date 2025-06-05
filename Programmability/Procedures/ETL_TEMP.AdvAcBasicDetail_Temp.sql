SET QUOTED_IDENTIFIER, ANSI_NULLS ON
GO

-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE  [ETL_TEMP].[AdvAcBasicDetail_Temp]  
	-- Add the parameters for the stored procedure here
	
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
SET NOCOUNT ON;

    -- Insert statements for procedure here
DECLARE  @vEffectivefrom  Int SET @vEffectiveFrom=(SELECT TimeKey FROM IBL_ENPA_DB_V2.[dbo].SYSDATAMATRIX WHERE CurrentStatus='C')
DECLARE @TimeKey  Int SET @TimeKey=(SELECT TimeKey FROM IBL_ENPA_DB_V2.[dbo].SYSDATAMATRIX WHERE CurrentStatus='C')
DECLARE @DATE AS DATE =(SELECT Date FROM IBL_ENPA_DB_V2.[dbo].SYSDATAMATRIX WHERE CurrentStatus='C')

------------------------update CREDIT CARD Product Details--------------------

--update IBL_ENPA_STGDB_V2.[dbo].[PISMO_CREDITCARDDETAILS] set Scheme_ProductCode = 'CREDIT CARD',FacilityType = 'TL'

--------------------------------------------------------------------------------------------------------------------------------------------------- 
  
  TRUNCATE TABLE TempAdvAcBasicDetail

  UPDATE IBL_ENPA_DB_V2.[dbo].[DimCurrency] set  SrcSysCurrencyCode=CurrencyCode where SrcSysCurrencyCode is null

  INSERT INTO TempAdvAcBasicDetail
           (BranchCode
			,AccountEntityId
			,CustomerEntityId
			,SystemACID
			,CustomerACID
			,GLAlt_Key
			,ProductAlt_Key
			,GLProductAlt_Key
			,FacilityType
			,SectorAlt_Key
			,SubSectorAlt_Key
			,ActivityAlt_Key
			,IndustryAlt_Key
			,SchemeAlt_Key
			,DistrictAlt_Key
			,AreaAlt_Key
			,VillageAlt_Key
			,StateAlt_Key
			,CurrencyAlt_Key
			,OriginalSanctionAuthAlt_Key
			,OriginalLimitRefNo
			,OriginalLimit
			,OriginalLimitDt
			,DtofFirstDisb
			,FlagReliefWavier
			,UnderLineActivityAlt_Key
			,MicroCredit
			,segmentcode
			,ScrCrError
			,AdjDt
			,AdjReasonAlt_Key
			,MarginType
			,Pref_InttRate
			,CurrentLimitRefNo
			,GuaranteeCoverAlt_Key
			,AccountName
			,AssetClass
			,JointAccount
			,LastDisbDt
			,ScrCrErrorBackup
			,AccountOpenDate
			,Ac_LADDt
			,Ac_DocumentDt
		    ,CurrentLimit
			,InttTypeAlt_Key
			,InttRateLoadFactor
			,Margin
			,CurrentLimitDt
			,Ac_DueDt
			,DrawingPowerAlt_Key
			,RefCustomerId
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
			,IsLAD
			,FincaleBasedIndustryAlt_key
			,AcCategoryAlt_Key
			,OriginalSanctionAuthLevelAlt_Key
			,AcTypeAlt_Key
			,ScrCrErrorSeq
			,BSRUNID
			,AdditionalProv
			,AclattestDevelopment
			,SourceAlt_Key
			,LoanSeries
			,LoanRefNo
			,SecuritizationCode
			,Full_Disb
			,OriginalBranchcode
			,ProjectCost
			,FlgSecured
			,ReferencePeriod
			,CountryofExposure
			)

--------LMS ---------

   SELECT   A.BranchCode
			,0 AccountEntityId
			,0 CustomerEntityId
			,A.CustomerACID SystemACID
			,A.CustomerACID CustomerACID
			,B.GLAlt_Key
			,C.ProductAlt_Key
			,NULL GLProductAlt_Key
			--,CASE WHEN SOURCESYSTEM='ECBF' AND  C.FacilityType IS NULL THEN 'TL' ELSE C.FacilityType  END  FacilityType
			,c.FacilityType --added by prashant----------------13122023-------------
			,NULL SectorAlt_Key
			,E.SubSectorAlt_Key
			,D.ActivityAlt_Key
			,F.IndustryAlt_Key
			,cf.Product_Key SchemeAlt_Key
			,NULL DistrictAlt_Key
			,NULL AreaAlt_Key
			,NULL VillageAlt_Key
			,NULL StateAlt_Key
			,G.CurrencyAlt_Key
			,NULL OriginalSanctionAuthAlt_Key
			,NULL OriginalLimitRefNo
			,NULL OriginalLimit
			,NULL OriginalLimitDt
			,A.FirstDtOfDisb DtofFirstDisb  
			,NULL FlagReliefWavier
			,NULL UnderLineActivityAlt_Key
			,NULL MicroCredit
			,AcSegmentCode segmentcode
			,NULL ScrCrError
			,NULL AdjDt
			,NULL AdjReasonAlt_Key
			,NULL MarginType
			,NULL Pref_InttRate
			,NULL CurrentLimitRefNo
			,NULL GuaranteeCoverAlt_Key
			,NULL AccountName
			,NULL AssetClass
			,NULL JointAccount
			,NULL LastDisbDt
			,NULL ScrCrErrorBackup
			,A.[AcOpenDt] AccountOpenDate
			,NULL Ac_LADDt
			,NULL Ac_DocumentDt
			,A.[CurrentLimit]+CASE WHEN A.AdhocExpiryDate>=@DATE THEN ISNULL(AdhocAmt,0) ELSE 0 END   CurrentLimit
			,NULL InttTypeAlt_Key
			,NULL InttRateLoadFactor
			,NULL Margin
			,NULL CurrentLimitDt
			,NULL Ac_DueDt
			,NULL DrawingPowerAlt_Key
			,cast(A.CustomerID as int) RefCustomerId
			,NULL AuthorisationStatus
			,@vEffectivefrom EffectiveFromTimeKey
			,49999 EffectiveToTimeKey
			,'SSISUSER' CreatedBy
			,GETDATE() DateCreated
			,NULL ModifiedBy
			,NULL DateModified
			,NULL ApprovedBy
			,NULL DateApproved
			,NULL D2Ktimestamp
			,NULL MocStatus
			,NULL MocDate
			,NULL MocTypeAlt_Key
			,CASE WHEN C.ProductGroup='FDSEC' THEN 1 ELSE NULL END IsLAD
			,NULL FincaleBasedIndustryAlt_key
			,NULL AcCategoryAlt_Key
			,NULL OriginalSanctionAuthLevelAlt_Key
			,NULL AcTypeAlt_Key
			,NULL ScrCrErrorSeq
			,NULL BSRUNID
			,NULL AdditionalProv
			,NULL AclattestDevelopment
			,S.SourceAlt_Key SourceAlt_Key
			,NULL LoanSeries
			,NULL LoanRefNo
			,NULL SecuritizationCode
			,NULL Full_Disb
			,NULL OriginalBranchcode
			,NULL ProjectCost
			,a.SecuredStatus SecuredStatus
			,CASE  WHEN NULLIF(A.AssetClassNorm,'') IS NULL THEN 91
					WHEN TRY_PARSE(A.AssetClassNorm AS INT) IN(90,365,180,60) 
					THEN  TRY_PARSE(A.AssetClassNorm AS INT) +1 
				ELSE TRY_PARSE(A.AssetClassNorm AS INT) END
				,NULL Country
      	FROM			IBL_ENPA_STGDB_V2.[dbo].ACCOUNT_ALL_SOURCE_SYSTEM A---- WHERE ISNUMERIC(AssetClassNorm)=0  
	LEFT JOIN		IBL_ENPA_DB_V2.[dbo].[DimGL] B ON A.GLCode=B.GLCode
					AND B.EffectiveFromTimeKey<=@TimeKey AND B.EffectiveToTimeKey>=@TimeKey
	LEFT JOIN		IBL_ENPA_DB_V2.[dbo].[DimProduct] C 
					ON C.ProductCode=A.Scheme_ProductCode		
					AND C.EffectiveFromTimeKey<=@TimeKey AND C.EffectiveToTimeKey>=@TimeKey
	LEFT JOIN		IBL_ENPA_DB_V2.[dbo].[DimActivity] D ON D.SrcSysActivityCode=A.[PurposeofAdvance]
					AND D.EffectiveFromTimeKey<=@TimeKey AND D.EffectiveToTimeKey>=@TimeKey
	LEFT JOIN		IBL_ENPA_DB_V2.[dbo].[DimSubSector] E ON E.SubSectorName=A.[Sector]
					AND E.EffectiveFromTimeKey<=@TimeKey AND E.EffectiveToTimeKey>=@TimeKey
	LEFT JOIN		IBL_ENPA_DB_V2.[dbo].[DimIndustry] F ON F.SrcSysIndustryCode=A.IndustryCode
					AND F.EffectiveFromTimeKey<=@TimeKey AND F.EffectiveToTimeKey>=@TimeKey
	LEFT JOIN		IBL_ENPA_DB_V2.[dbo].[DimCurrency] G ON G.SrcSysCurrencyCode=A.CurrencyCode
					AND G.EffectiveFromTimeKey<=@TimeKey AND G.EffectiveToTimeKey>=@TimeKey
	LEFT JOIN		IBL_ENPA_DB_V2.[dbo].DIMSOURCEDB S ON S.SourceName=A.SourceSystem
					AND S.EffectiveFromTimeKey<=@TimeKey AND S.EffectiveToTimeKey>=@TimeKey
	
	Left join  IBL_ENPA_DB_V2.[dbo].[DimProduct] Cf On A.Scheme_ProductCode=cf.ProductCode                       --------------Newly added by kapil on 28/02/2024
		      AND Cf.EffectiveFromTimeKey<=@TimeKey AND CF.EffectiveToTimeKey>=@TimeKey 
	
END

---GO
 /*********************************************************************************************************/
/*  Existing Customers Account Entity ID Update  */

DELETE from IBL_ENPA_TEMPDB_V2.dbo.TempAdvAcBasicDetail where CustomerACID=''

UPDATE TEMP 
SET TEMP.AccountEntityId=MAIN.AccountEntityId
FROM  IBL_ENPA_TEMPDB_V2.DBO.TempAdvAcBasicDetail TEMP
INNER JOIN IBL_ENPA_DB_V2.[DBO].[AdvAcBasicDetail] MAIN 
ON TEMP.CustomerAcId=MAIN.CustomerAcId
WHERE MAIN.EffectiveToTimeKey=49999


--GO
/*********************************************************************************************************/
/*  New Customers Account Entity ID Update  */
DECLARE @AccountEntityId INT=0 
SELECT @AccountEntityId=MAX(ISNULL(AccountEntityId,0)) from (
select MAX(ISNULL(AccountEntityId,0))AccountEntityId FROM  IBL_ENPA_DB_V2.[dbo].[AdvAcBasicDetail]  UNION
--select MAX(ISNULL(AccountEntityId,0)) FROM  IBL_ENPA_DB_V2.[dbo].[AdvNFAcBasicDetail]  UNION
select MAX(ISNULL(AccountEntityId,0)) FROM  IBL_ENPA_TEMPDB_V2.[dbo].[TempAdvAcBasicDetail]  UNION
select MAX(ISNULL(AccountEntityId,0)) FROM  IBL_ENPA_TEMPDB_V2.[dbo].[TempAdvNFAcBasicDetail]  )x
IF @AccountEntityId IS NULL  
BEGIN
SET @AccountEntityId=0
END
 
 DROP TABLE IF EXISTS #ACENT
 SELECT CustomerAcId,(@AccountEntityId + ROW_NUMBER()OVER(ORDER BY (SELECT 1))) AccountEntityId
	INTO	#ACENT
			FROM IBL_ENPA_TEMPDB_V2.DBO.TempAdvAcBasicDetail
			WHERE AccountEntityId=0 OR AccountEntityId IS NULL

UPDATE TEMP 
SET TEMP.AccountEntityId=ACCT.AccountEntityId
 FROM IBL_ENPA_TEMPDB_V2.DBO.TempAdvAcBasicDetail TEMP
INNER JOIN #ACENT ACCT ON TEMP.CustomerAcId=ACCT.CustomerAcId


/*********************************************************************************************************/
--/*  Existing Customers Ac Key ID Update  */

--UPDATE TEMP 
--SET TEMP.Ac_Key=MAIN.Ac_Key
 
--FROM  IBL_ENPA_TEMPDB_V2.DBO.[TempAdvAcBasicDetail] TEMP
--INNER JOIN IBL_ENPA_DB_V2.[DBO].[AdvAcBasicDetail] MAIN ON TEMP.CustomerAcId=MAIN.CustomerAcId
--WHERE MAIN.EffectiveToTimeKey=49999

----GO
--/*********************************************************************************************************/
--/*  New Customers Ac Key ID Update  */
--DECLARE @Ac_Key BIGINT=0 
--SELECT @Ac_Key=MAX(Ac_Key) FROM  IBL_ENPA_DB_V2.[dbo].[AdvAcBasicDetail] 
--IF @Ac_Key IS NULL  
--BEGIN
--SET @Ac_Key=0
--END
 
--UPDATE TEMP 
--SET TEMP.Ac_Key=ACCT.Ac_Key
-- FROM IBL_ENPA_TEMPDB_V2.DBO.[TempAdvAcBasicDetail] TEMP
--INNER JOIN (SELECT CustomerAcId,(@Ac_Key + ROW_NUMBER()OVER(ORDER BY (SELECT 1))) Ac_Key
--			FROM IBL_ENPA_TEMPDB_V2.DBO.[TempAdvAcBasicDetail]
--			WHERE Ac_Key=0 OR Ac_Key IS NULL)ACCT ON TEMP.CustomerAcId=ACCT.CustomerAcId


--GO

---Remove Duplicate----(Vinit Tamgadge)
;with cta as
(select ROW_NUMBER() over (partition by AccountEntityId order by AccountEntityId) ACID,
* from TempAdvAcBasicDetail)  
Delete from Cta  where ACID>1



GO