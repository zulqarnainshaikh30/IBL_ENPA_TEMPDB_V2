SET QUOTED_IDENTIFIER, ANSI_NULLS ON
GO




-- =============================================  
-- Author:  <Author,,Name>  
-- Create date: <Create Date,,>  
-- Description: <Description,,>  
-- =============================================  
CREATE PROCEDURE [ETL_TEMP].[AdvSecurityDetail_Temp]   
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
  
  

  TRUNCATE TABLE TempAdvSecurityDetail  --select * from TempAdvSecurityDetail order by SecurityEntityID 
 
   ------------- SECURITY  ACCOUNT  ---------  

  INSERT INTO TempAdvSecurityDetail  
   (
   AccountEntityId  
   ,CustomerEntityId  
   ,SecurityType  
   ,CollateralType  
   ,SecurityAlt_Key  
   ,SecurityEntityID  
   ,Security_RefNo  
   ,SecurityNature  
   ,SecurityChargeTypeAlt_Key  
   ,CurrencyAlt_Key  
   ,EntryType  
   ,ScrCrError  
   ,InwardNo  
   ,Limitnode_Flag  
   ,RefCustomerId  
   ,RefSystemAcId  
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
   ,MocTypeAlt_Key  
   ,MocStatus  
   ,MocDate  
   ,SecurityParticular  
   ,OwnerTypeAlt_Key  
   ,AssetOwnerName  
   ,ValueAtSanctionTime  
   ,BranchLastInspecDate  
   ,SatisfactionNo  
   ,SatisfactionDate  
   ,BankShare  
   ,ActionTakenRemark  
   ,SecCharge  
   ,CollateralID
   )   
   -------------   LMS   ---------  
   SELECT   
   D.AccountEntityId  
   ,B.CustomerEntityId  
   ,'P' SecurityType  
   ,NULL CollateralType  
   ,C.CollateralSubTypeAltKey AS  SecurityAlt_Key  
   ,0 SecurityEntityID  
   ,Security_ID Security_RefNo  
   ,NULL SecurityNature  
   ,null SecurityChargeTypeAlt_Key  
   ,NULL CurrencyAlt_Key  
   ,NULL EntryType  
   ,NULL ScrCrError  
   ,NULL InwardNo  
   ,NULL Limitnode_Flag  
   ,B.CustomerID  
   ,D.CustomerACID SystemAcId  
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
   ,NULL MocTypeAlt_Key  
   ,NULL MocStatus  
   ,NULL MocDate  
   ,NULL SecurityParticular  
   ,NULL OwnerTypeAlt_Key  
   ,NULL AssetOwnerName  
   ,NULL ValueAtSanctionTime  
   ,NULL BranchLastInspecDate  
   ,NULL SatisfactionNo  
   ,NULL SatisfactionDate  
   ,NULL BankShare  
   ,NULL ActionTakenRemark  
   ,NULL SecCharge  
   ,NULL CollateralID --select * 
 FROM   IBL_ENPA_STGDB_V2.dbo.Security_All_Source_System A   
 INNER JOIN  TempAdvAcBasicDetail D 
 ON A.AccountID=D.CustomerACID   AND D.EffectiveFromTimeKey<=@TimeKey AND D.EffectiveToTimeKey>=@TimeKey And D.SourceAlt_Key=1
 INNER JOIN  TempCustomerBasicDetail B 
 ON D.CustomerEntityId=B.CustomerEntityId   AND B.EffectiveFromTimeKey<=@TimeKey AND B.EffectiveToTimeKey>=@TimeKey 	
 LEFT JOIN   IBL_ENPA_DB_V2.[dbo].[DimCollateralSubType] C ON C.SrcSecurityCode=Isnull(A.SecurityCode,'Others')  
 AND C.EffectiveFromTimeKey<=@TimeKey AND C.EffectiveToTimeKey>=@TimeKey  	
  WHERE 
  C.Valid='Y' and C.SourceAlt_Key=1 --and X.CustomerEntityId is NULL --select * from IBL_ENPA_DB_V2.[dbo].[DimCollateralSubType] --select SourceAlt_Key,* from TempAdvAcBasicDetail
  

UPDATE TEMP 
SET TEMP.SecurityEntityId=MAIN.SecurityEntityId 
FROM  IBL_ENPA_TEMPDB_V2.DBO.[TempAdvSecurityDetail] TEMP
INNER JOIN IBL_ENPA_DB_V2.CurDat.[AdvSecurityDetail] MAIN 
ON TEMP.AccountEntityId=MAIN.AccountEntityId
--AND TEMP.Security_RefNo=MAIN.Security_RefNo			
WHERE MAIN.EffectiveToTimeKey=49999

--GO     
/*********************************************************************************************************/
/*  New Customers Account Entity ID Update  */
DECLARE @SecurityEntityId INT=0 
SELECT @SecurityEntityId=  MAX(SecurityEntityId) FROM  IBL_ENPA_DB_V2.CurDat.[AdvSecurityDetail] --select * from IBL_ENPA_DB_V2.[dbo].[AdvSecurityDetail]
IF @SecurityEntityId IS NULL  
BEGIN
SET @SecurityEntityId=0
END
 
 
UPDATE TEMP 
SET TEMP.SecurityEntityId=ACCT.SecurityEntityId
 FROM IBL_ENPA_TEMPDB_V2.DBO.[TempAdvSecurityDetail] TEMP
INNER JOIN (SELECT AccountEntityid,Security_RefNo,(0 + ROW_NUMBER()OVER(ORDER BY (SELECT 1))) SecurityEntityId
			FROM IBL_ENPA_TEMPDB_V2.DBO.[TempAdvSecurityDetail]
			WHERE SecurityEntityId=0 OR SecurityEntityId IS NULL)
		ACCT 
		ON TEMP.Security_RefNo=ACCT.Security_RefNo
		AND TEMP.AccountEntityId		=ACCT.AccountEntityid		

 			
UPDATE TEMP 
SET TEMP.SecurityEntityId=ACCT.SecurityEntityId
 FROM IBL_ENPA_TEMPDB_V2.DBO.[TempAdvSecurityDetail] TEMP
INNER JOIN (SELECT CustomerEntityid,Security_RefNo,(0 + ROW_NUMBER()OVER(ORDER BY (SELECT 1))) SecurityEntityId
			FROM IBL_ENPA_TEMPDB_V2.DBO.[TempAdvSecurityDetail]
			WHERE SecurityEntityId=0 OR SecurityEntityId IS NULL)
		ACCT 
		ON TEMP.Security_RefNo=ACCT.Security_RefNo
		AND TEMP.CustomerEntityid		=ACCT.CustomerEntityid		
		


		---Update By Vinit--
		--;with CTE As(
		--select Row_number() over (partition by SecurityEntityID order by SecurityEntityID) SEID ,* from TempAdvSecurityDetail)
		--delete from CTE   where SEID>1
  
 END
GO