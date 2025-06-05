SET QUOTED_IDENTIFIER, ANSI_NULLS ON
GO

-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [ETL_TEMP].[AdvAcRelation_Temp] 
	-- Add the parameters for the stored procedure here
	
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

    -- Insert statements for procedure here
	

TRUNCATE TABLE TempAdvAcRelations 

DECLARE  @vEffectivefrom  Int SET @vEffectiveFrom=(SELECT TimeKey FROM IBL_ENPA_DB_V2.[dbo].SYSDATAMATRIX WHERE CurrentStatus='C')
DECLARE @TimeKey  Int SET @TimeKey=(SELECT TimeKey FROM IBL_ENPA_DB_V2.[dbo].SYSDATAMATRIX WHERE CurrentStatus='C')
DECLARE @DATE AS DATE =(SELECT Date FROM IBL_ENPA_DB_V2.[dbo].SYSDATAMATRIX WHERE CurrentStatus='C')


INSERT INTO TempAdvAcRelations
				(BranchCode,
				 RelationEntityId,
				 CustomerEntityId,
				 AccountEntityId,
				 RelationTypeAlt_Key,
				 RelationSrNo,
				 RelationshipAuthorityCodeAlt_Key,
				 InwardNo,
				 FacilityNo,
				 GuaranteeValue,
				 RefCustomerID,
				 RefSystemAcId,
				 EffectiveFromTimeKey,
				 EffectiveToTimeKey,
				 CreatedBy,
				 DateCreated)

/*  THIS PART OF QUERY IS FOR Joint Borrower and Guarantor */

SELECT 
 TACBD.BranchCode, 
 NULL As RelationEntityId, 
 ISNULL(TACBD.CustomerEntityId,0) AS CustomerEntityId , 
 ISNULL(TACBD.AccountEntityId,0) AS AccountEntityId , 
 NULL As RelationTypeAlt_Key, 
 NULL As RelationSrNo, 
 NULL As RelationshipAuthorityCodeAlt_Key, 
 NULL As InwardNo, 
 NULL As FacilityNo,
 NULL As GuaranteeValue, 
 AAS.CustomerID As RefCustomerID ,
 AAS.CustomerACID As RefSystemAcId,
 @Timekey,
 49999,
 'SSISUSER',
 Convert(Date, Getdate(),103)
  
From IBL_ENPA_STGDB_V2.DBO.ACCOUNT_ALL_SOURCE_SYSTEM AAS INNER JOIN TempAdvAcBasicDetail TACBD  -- changed on date 21/02/2024
on AAS.CustomerACID = TACBD.CustomerACID


--FROM IBL_ENPA_STGDB_V2.DBO.CUSTOMERRELATION_SOURCESYSTEM_STG AAS INNER JOIN TempAdvAcBasicDetail TACBD  --Previously We use this changed on date 21/02/2024
--on AAS.AccountID = TACBD.CustomerACID

----------------------------------------------------------------------------------------------------------------
 
update TAAR
SET RelationEntityId = TCBD.RelationEntityId
FROM IBL_ENPA_DB_V2.CurDat.AdvCustRelationship TCBD
INNER JOIN TempAdvAcRelations TAAR
ON TCBD.RefCustomerID = TAAR.RefCustomerID 
and TCBD.EffectiveFromTimeKey<=@Timekey AND TCBD.EffectiveToTimeKey>=@Timekey
 
 ------------------------------------------------------------------------------------------------------------------
DECLARE @MAX INT
SELECT  @MAX=   MAX(RelationEntityId) FROM (SELECT MAX(RelationEntityId)RelationEntityId FROM IBL_ENPA_DB_V2.CurDat.AdvCustRelationship
											--UNION ALL
											--SELECT MAX(RelationEntityId)RelationEntityId FROM IBL_ENPA_DB_V2.DBO.ADVCUSTRELATIONSHIP_MOD
                                           )A
   
IF @MAX IS NULL  
BEGIN
SET @MAX=0
END

UPDATE A
SET RelationEntityId = B.ReletionEntityId

FROM TempAdvAcRelations A
INNER JOIN (SELECT RefCustomerId,
  ISNULL(@MAX,0)+ROW_NUMBER() OVER(  ORDER BY RefCustomerId) AS ReletionEntityId
   FROM TempAdvAcRelations A
    WHERE RefCustomerId IS NOT NULL
 GROUP BY RefCustomerId
   
   )B ON A.RefCustomerId=B.RefCustomerId
WHERE A.RelationEntityId IS NULL

------------------------------------------------------------------------------------------------------------------
DECLARE @MAX1 INT    
select @MAX1=MAX(RelationEntityId) FROM (SELECT MAX(RelationEntityId)RelationEntityId FROM IBL_ENPA_DB_V2.CurDat.AdvCustRelationship
										 --UNION ALL
										 --SELECT MAX(RelationEntityId)RelationEntityId FROM IBL_ENPA_DB_V2.DBO.ADVCUSTRELATIONSHIP_MOD
                                           )A
IF @MAX1 IS NULL  
BEGIN
SET @MAX1=0
END
UPDATE TempAdvAcRelations
SET @MAX1=RelationEntityId=@MAX1+1
WHERE RelationEntityId IS NULL

------------------------------------------------------------------------------------------------------------------
Update TAAR
set RelationTypeAlt_Key = (CASE WHEN TCBD.ConstitutionAlt_Key IN (410,421) THEN 16 ELSE 15 END)

FROM TempAdvAcRelations TAAR
INNER JOIN .IBL_ENPA_DB_V2.dbo.CustomerBasicDetail TCBD ON  cast(TAAR.refcustomerid as int) = Cast(TCBD.customerID as int)
 
 ------------------------------------------------------------------------------------------------------------------

 ------------------------------------------------------------------------------------------------------------------
UPDATE TEmpAdvAcRelations  SET RelationSrNo = 0
 ------------------------------------------------------------------------------------------------------------------
Update TAAR
SET RelationSrNo  = CASE WHEN RelationTypeAlt_Key in(15,16) THEN 1 ELSE 0 END ,
	RelationshipAuthorityCodeAlt_Key = CASE WHEN RelationTypeAlt_Key=17 THEN 42 ELSE 0 END

from TempAdvAcRelations TAAR
WHERE EffectiveToTimeKey = 49999 AND RelationTypeAlt_Key in(15,16,17)

 ------------------------------------------------------------------------------------------------------------------
UPDATE a

 SET RelationshipAuthorityCodeAlt_Key=0
 FROM TempAdvAcRelations a
 WHERE RelationshipAuthorityCodeAlt_Key is null 

 ------------------------------------------------------------------------------------------------------------------
END


GO