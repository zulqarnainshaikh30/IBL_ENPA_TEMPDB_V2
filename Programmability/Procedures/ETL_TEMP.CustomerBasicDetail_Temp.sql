SET QUOTED_IDENTIFIER, ANSI_NULLS ON
GO

CREATE PROCEDURE [ETL_TEMP].[CustomerBasicDetail_Temp]  
AS
BEGIN

	SET NOCOUNT ON;

    -- Insert statements for procedure here
TRUNCATE TABLE TempCustomerBasicDetail
  
DECLARE  @vEffectivefrom  Int SET @vEffectiveFrom=(SELECT TimeKey FROM IBL_ENPA_DB_V2.[dbo].SYSDATAMATRIX WHERE CurrentStatus='C')
DECLARE @TimeKey  Int SET @TimeKey=(SELECT TimeKey FROM IBL_ENPA_DB_V2.[dbo].SYSDATAMATRIX WHERE CurrentStatus='C')
DECLARE @DATE AS DATE =(SELECT Date FROM IBL_ENPA_DB_V2.[dbo].SYSDATAMATRIX WHERE CurrentStatus='C')

--------------------------------------------------------------------------------------------------------------------------------------------------- 
SET DATEFORMAT DMY
INSERT INTO [TempCustomerBasicDetail] 
(	       
CustomerEntityId
,CustomerId
,D2kCustomerid
,ParentBranchCode
,CustomerName
,CustomerInitial
,CustomerSinceDt
--,ConsentObtained
,ConstitutionAlt_Key
,OccupationAlt_Key
,ReligionAlt_Key
,CasteAlt_Key
,FarmerCatAlt_Key
,GaurdianSalutationAlt_Key
,GaurdianName
,GuardianType
,CustSalutationAlt_Key
,MaritalStatusAlt_Key
--,DegUpgFlag
--,ProcessingFlag
--,MOCLock
--,MoveNpaDt
,AssetClass
,BIITransactionCode
,D2K_REF_NO
--,CustomerNameBackup
--,ScrCrErrorBackup
,ScrCrError
,ReferenceAcNo
,CustCRM_RatingAlt_Key
,CustCRM_RatingDt
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
,FLAG
,MocStatus
,MocDate
--,BaselProcessing
,MocTypeAlt_Key
,CommonMocTypeAlt_Key
,LandHolding
,ScrCrErrorSeq
--,CustType
--,ServProviderAlt_Key
--,NonCustTypeAlt_Key
,Remark
,UCIF_ID
,SourceSystemAlt_Key
) 
------------------------LMS SYSTEM DATA----------------------
SELECT 
 0 AS  CustomerEntityId
,C.CustomerId AS CustomerId
,C.CustomerId AS D2kCustomerid
,NULL AS ParentBranchCode
,LTRIM(C.CustomerName) AS CustomerName
,NULL AS CustomerInitial
,NULL AS CustomerSinceDt---DAO
--,'Y' AS ConsentObtained
,0 AS ConstitutionAlt_Key  ---STBLW9ENTTYPE  (master STBLW9ENTTYPE) if blank then INDIVIDUAL 
,0 AS OccupationAlt_Key  ---OCC  (master  UTBLOC)
,0 AS ReligionAlt_Key
,0 AS CasteAlt_Key
,0 AS FarmerCatAlt_Key
,NULL AS GaurdianSalutationAlt_Key
,NULL AS GaurdianName
,NULL AS GuardianType
,0 AS CustSalutationAlt_Key
,0 AS MaritalStatusAlt_Key
--,NULL AS DegUpgFlag
--,0 AS ProcessingFlag
--,NULL AS MOCLock
--,NULL AS MoveNpaDt
,NULL AS AssetClass
,NULL AS BIITransactionCode
,NULL AS D2K_REF_NO
--,NULL AS CustomerNameBackup
--,NULL AS ScrCrErrorBackup
,NULL AS ScrCrError
,NULL AS ReferenceAcNo
,NULL AS CustCRM_RatingAlt_Key
,NULL AS CustCRM_RatingDt
,NULL AS AuthorisationStatus
,@VEFFECTIVEFROM AS EFFECTIVEFROMTIMEKEY 
,49999 AS EFFECTIVETOTIMEKEY
,'SSISUSER' ASCREATEDBY
,GETDATE() AS DATECREATED
,NULL AS ModifiedBy
,NULL AS DateModified
,NULL AS ApprovedBy
,NULL AS DateApproved
,NULL AS D2Ktimestamp
,NULL AS FLAG
,NULL AS MocStatus
,NULL AS MocDate
--,NULL AS BaselProcessing
,NULL AS MocTypeAlt_Key
,NULL AS CommonMocTypeAlt_Key
,NULL AS LandHolding
,NULL AS ScrCrErrorSeq
--,NULL AS CustType
--,NULL AS ServProviderAlt_Key
--,NULL AS NonCustTypeAlt_Key
,NULL AS Remark
,UCIC_ID
,S.SourceAlt_Key
--select *
FROM  IBL_ENPA_STGDB_V2.DBO.CUSTOMER_ALL_SOURCE_SYSTEM C  --first used this table
INNER JOIN  (
				SELECT DISTINCT REFCUSTOMERID FROM TempAdvAcBasicDetail
				UNION
			    SELECT DISTINCT REFCUSTOMERID FROM TempAdvNFAcBasicDetail
			)	ACBD  ON C.CUSTOMERID = ACBD.REFCUSTOMERID
LEFT JOIN IBL_ENPA_DB_V2.DBO.DIMSOURCEDB S
	ON S.SourceName=C.SourceSystem
	AND S.EffectiveToTimeKey=49999

	
 
 /*********************************************************************************************************/
/*  Existing Customers Customer Entity ID Update  */

UPDATE TEMP 
SET TEMP.CustomerEntityId=MAIN.CustomerEntityId
 
FROM  [TempCustomerBasicDetail] TEMP
INNER JOIN IBL_ENPA_DB_V2.[CurDat].[CustomerBasicDetail] MAIN ON TEMP.CustomerId=MAIN.CustomerId
WHERE MAIN.EffectiveToTimeKey=49999


/*********************************************************************************************************/
/*  New Customers Customer Entity ID Update  */
DECLARE @CustomerEntityId INT=0 
SELECT @CustomerEntityId=MAX(CustomerEntityId) FROM  IBL_ENPA_DB_V2.[dbo].[CustomerBasicDetail] 
IF @CustomerEntityId IS NULL  
BEGIN
SET @CustomerEntityId=0
END
 
 
DROP TABLE IF EXISTS #CustEntityID 
SELECT CustomerId,(@CustomerEntityId + ROW_NUMBER()OVER(ORDER BY (SELECT 1))) CustomerEntityId
	into #CustEntityID
			FROM [TempCustomerBasicDetail]
			WHERE CustomerEntityId=0 OR CustomerEntityId IS NULL  
 
UPDATE TEMP 
SET TEMP.CustomerEntityId=ACCT.CustomerEntityId
 FROM  [TempCustomerBasicDetail] TEMP
INNER JOIN #CustEntityID ACCT ON TEMP.CustomerId=ACCT.CustomerId

DROP TABLE IF EXISTS #CustEntityID 

 /*********************************************************************************************************/
/*  Existing Customers Customer UCIF Entity ID Update  */

UPDATE TEMP 
SET TEMP.UcifEntityID=MAIN.UcifEntityID
 
FROM  [TempCustomerBasicDetail] TEMP
INNER JOIN IBL_ENPA_DB_V2.[CurDat].[CustomerBasicDetail] MAIN ON TEMP.UCIF_ID=MAIN.UCIF_ID
WHERE MAIN.EffectiveToTimeKey=49999


/*********************************************************************************************************/
/*  New Customers Customer Entity ID Update  */
DECLARE @UcifEntityID INT=0 
SELECT @UcifEntityID=MAX(UcifEntityID) FROM  IBL_ENPA_DB_V2.[dbo].[CustomerBasicDetail] 
IF @UcifEntityID IS NULL  
BEGIN
SET @UcifEntityID=0
END
 
DROP TABLE IF EXISTS #UcifEntityID 
SELECT UCIF_ID,(@UcifEntityID + ROW_NUMBER()OVER(ORDER BY (SELECT 1))) UcifEntityID
	INTO #UcifEntityID
			FROM (select UCIF_ID from [TempCustomerBasicDetail]  
					WHERE UcifEntityID=0 OR UcifEntityID IS NULL
					GROUP BY UCIF_ID
				  )A

UPDATE TEMP 
SET TEMP.UcifEntityID=ACCT.UcifEntityID
 FROM  [TempCustomerBasicDetail] TEMP
INNER JOIN #UcifEntityID
			ACCT
		 ON TEMP.UCIF_ID=ACCT.UCIF_ID


DROP TABLE IF EXISTS #UcifEntityID 



/***********************************************************************************************************/
/*  Updating CustomerEntityID In AdvAcBasciDetail  */

UPDATE ACBD SET ACBD.CustomerEntityId=CBD.CustomerEntityId 
FROM TempCustomerBasicDetail CBD
INNER JOIN TempAdvAcBasicDetail ACBD ON CBD.CustomerId=(CASE WHEN ACBD.RefCustomerId IS NULL THEN ACBD.CustomerAcID ELSE ACBD.RefCustomerId END)


UPDATE ACBD SET ACBD.CustomerEntityId=CBD.CustomerEntityId 
FROM TempCustomerBasicDetail CBD
INNER JOIN TEMPADVNFACBASICDETAIL ACBD ON CBD.CustomerId=(CASE WHEN ACBD.RefCustomerId IS NULL THEN ACBD.CustomerAcID ELSE ACBD.RefCustomerId END)
/*********************************************************************************************************/

--------------------------------------------------------------------------------------------------------------------------
/* UPDATING DATA VALUE NOT PROVIDED */

 UPDATE TempCustomerBasicDetail SET  CasteAlt_Key = 0 where CasteAlt_Key IS NULL
 
 UPDATE TempCustomerBasicDetail SET  ReligionAlt_Key = 0 where ReligionAlt_Key IS NULL
 
 UPDATE TempCustomerBasicDetail SET  FarmerCatAlt_Key = 0 where FarmerCatAlt_Key IS NULL
 
 UPDATE TempCustomerBasicDetail SET  CustSalutationAlt_Key = 0 where CustSalutationAlt_Key IS NULL
 
 UPDATE TempCustomerBasicDetail SET  ConstitutionAlt_Key = 0 where ConstitutionAlt_Key IS NULL

 ;with cte as (select *,ROW_NUMBER() OVER (PARTITION BY CustomerEntityid ORDER BY CustomerEntityid) as Rnk from TempCustomerBasicDetail) delete from cte where Rnk > 1
 
-------------------------------------------------------------------------------------------------------------------------- 
END

GO