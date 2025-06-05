SET QUOTED_IDENTIFIER, ANSI_NULLS ON
GO




-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [ETL_TEMP].[AdvAcNFBasicDetail_Temp] 
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

--------------------------------------------------------------------------------------------------------------------------------------------------- 
  
  TRUNCATE TABLE TempAdvNFAcBasicDetail

  UPDATE IBL_ENPA_DB_V2.[dbo].[DimCurrency] set  SrcSysCurrencyCode=CurrencyCode where SrcSysCurrencyCode is null

  INSERT INTO TempAdvNFAcBasicDetail
           (
					BranchCode
					,CustomerEntityId
					,AccountEntityId
					,SystemACID
					,CustomerACID
					,D2KAcid
					,ProductAlt_Key
					,GLProductAlt_Key
					,FacilityType
					,SectorAlt_Key
					,SubSectorAlt_Key
					,ActivityAlt_Key
					,IndustryAlt_Key
					,DistrictAlt_Key
					,AreaAlt_Key
					,StateAlt_Key
					,CurrencyAlt_Key
					,OriginalSanctionAuthAlt_Key
					,OriginalLimitRefNo
					,OriginalLimit
					,OriginalLimitDt
					,DtofFirstDisb
					,ScrCrError
					,AdjDt
					,AdjReasonAlt_Key
					,VillageCode
					,SplCatg1Alt_Key
					,SplCatg2Alt_Key
					,CreditRatingScore
					,CreditRatingDt
					,SanctionReferenceNo
					,GuaranteeCoverAlt_Key
					,JointAccount
					,ProcessingFeeApplicable
					,ProcessingFeeAmt
					,ProcessingFeeRecoveryAmt
					,LimitRefNo
					,Ac_LADDt
					,Ac_DocumentDt
					,Ac_CreditRatingAlt_Key
					,CurrentLimit
					,CurrentLimitDt
					,AccountOpenDate
					,EmpCode
					,CurrentLimitRefNo
					,MarginType
					,Margin
					,RefCustomerID
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
					,SplCatg3Alt_Key
					,SplCatg4Alt_Key
					,NonFundCategoryAlt_Key
					,FacilitiesNo
					,BankAlt_Key
					,AcCategoryAlt_Key
					,OriginalSanctionAuthLevelAlt_Key
					,AcTypeAlt_Key
					,ScrCrErrorSeq
					,Old_Mapkey
					--,D2k_OLDAscromID
					,BSRUNID
					,CustomerId
					,CustomerName
					,segmentcode
					,SourceAlt_Key
			)

   -------------    LMS  ---------
   SELECT			A.BranchCode
					,0 CustomerEntityId
					,0 AccountEntityId
					,A.CustomerACID as SystemACID
					,A.CustomerACID
					,NULL D2KAcid
					,ProductAlt_Key
					,NULL GLProductAlt_Key
					,C.FacilityType
					,NULL SectorAlt_Key
					,SubSectorAlt_Key
					,ActivityAlt_Key
					,F.IndustryAlt_Key
					,NULL DistrictAlt_Key
					,NULL AreaAlt_Key
					,NULL StateAlt_Key
					,CurrencyAlt_Key
					,NULL OriginalSanctionAuthAlt_Key
					,NULL OriginalLimitRefNo
					,NULL OriginalLimit
					,NULL OriginalLimitDt
					,FirstDtOfDisb DtofFirstDisb
					,NULL ScrCrError
					,NULL AdjDt
					,NULL AdjReasonAlt_Key
					,NULL VillageCode
					,NULL SplCatg1Alt_Key
					,NULL SplCatg2Alt_Key
					,NULL CreditRatingScore
					,NULL CreditRatingDt
					,NULL SanctionReferenceNo
					,NULL GuaranteeCoverAlt_Key
					,NULL JointAccount
					,NULL ProcessingFeeApplicable
					,NULL ProcessingFeeAmt
					,NULL ProcessingFeeRecoveryAmt
					,NULL LimitRefNo
					,NULL Ac_LADDt
					,NULL Ac_DocumentDt
					,NULL Ac_CreditRatingAlt_Key
					,CurrentLimit
					,NULL CurrentLimitDt
					,[AcOpenDt] AccountOpenDate
					,NULL EmpCode
					,NULL CurrentLimitRefNo
					,NULL MarginType
					,NULL Margin
					,A.CustomerID RefCustomerID
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
					,NULL SplCatg3Alt_Key
					,NULL SplCatg4Alt_Key
					,NULL NonFundCategoryAlt_Key
					,NULL FacilitiesNo
					,NULL BankAlt_Key
					,NULL AcCategoryAlt_Key
					,NULL OriginalSanctionAuthLevelAlt_Key
					,NULL AcTypeAlt_Key
					,NULL ScrCrErrorSeq
					,NULL Old_Mapkey
					--,SubAssetClassCode AS  D2k_OLDAscromID IINEED TO CHECK
					,NULL BSRUNID
					,A.CustomerId
					,NULL CustomerName
					,AcSegmentCode segmentcode
					,S.SourceAlt_Key SourceAlt_Key
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
	WHERE			C.FacilityType IN ('NF','LC','BG')
	

END

---GO
 /*********************************************************************************************************/
/*  Existing Customers Account Entity ID Update  */

DELETE from IBL_ENPA_TEMPDB_V2.dbo.TempAdvNFAcBasicDetail where CustomerACID=''

UPDATE TEMP 
SET TEMP.AccountEntityId=MAIN.AccountEntityId
FROM  IBL_ENPA_TEMPDB_V2.DBO.TempAdvNFAcBasicDetail TEMP
INNER JOIN IBL_ENPA_DB_V2.[DBO].[AdvNFAcBasicDetail] MAIN 
ON TEMP.CustomerAcId=MAIN.CustomerAcId
WHERE MAIN.EffectiveToTimeKey=49999


--GO
/*********************************************************************************************************/

DECLARE @AccountEntityId INT=0 
SELECT @AccountEntityId= MAX(ISNULL(AccountEntityId,0)) from (
select MAX(ISNULL(AccountEntityId,0))AccountEntityId FROM  IBL_ENPA_DB_V2.[dbo].[AdvAcBasicDetail]  UNION
select MAX(ISNULL(AccountEntityId,0)) FROM  IBL_ENPA_DB_V2.[dbo].[AdvNFAcBasicDetail]  UNION
select MAX(ISNULL(AccountEntityId,0)) FROM  IBL_ENPA_TEMPDB_V2.[dbo].[TempAdvAcBasicDetail]  UNION
select MAX(ISNULL(AccountEntityId,0)) FROM  IBL_ENPA_TEMPDB_V2.[dbo].[TempAdvNFAcBasicDetail]  )x
IF @AccountEntityId IS NULL  
BEGIN
SET @AccountEntityId=0
END
 
 DROP TABLE IF EXISTS #ACENT
 SELECT CustomerAcId,(@AccountEntityId + ROW_NUMBER()OVER(ORDER BY (SELECT 1))) AccountEntityId
	INTO	#ACENT
			FROM IBL_ENPA_TEMPDB_V2.DBO.TempAdvNFAcBasicDetail
			WHERE AccountEntityId=0 OR AccountEntityId IS NULL

UPDATE TEMP 
SET TEMP.AccountEntityId=ACCT.AccountEntityId
 FROM IBL_ENPA_TEMPDB_V2.DBO.TempAdvNFAcBasicDetail TEMP
INNER JOIN #ACENT ACCT ON TEMP.CustomerAcId=ACCT.CustomerAcId

UPDATE IBL_ENPA_TEMPDB_V2.DBO.TempAdvNFAcBasicDetail SET D2k_OLDAscromID='STD' WHERE D2k_OLDAscromID IS NULL
/*********************************************************************************************************/
--/*  Existing Customers Ac Key ID Update  */

--UPDATE TEMP 
--SET TEMP.Ac_Key=MAIN.Ac_Key
 
--FROM  IBL_ENPA_TEMPDB_V2.DBO.[TempAdvAcNFBasicDetail] TEMP
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
-- FROM IBL_ENPA_TEMPDB_V2.DBO.[TempAdvAcNFBasicDetail] TEMP
--INNER JOIN (SELECT CustomerAcId,(@Ac_Key + ROW_NUMBER()OVER(ORDER BY (SELECT 1))) Ac_Key
--			FROM IBL_ENPA_TEMPDB_V2.DBO.[TempAdvAcNFBasicDetail]
--			WHERE Ac_Key=0 OR Ac_Key IS NULL)ACCT ON TEMP.CustomerAcId=ACCT.CustomerAcId


--GO





GO