SET QUOTED_IDENTIFIER, ANSI_NULLS ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [ETL_TEMP].[AdvCustCommunicationDetail_Temp] 
	-- Add the parameters for the stored procedure here 
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON; 
    -- Insert statements for procedure here   
TRUNCATE TABLE TempAdvCustCommunicationDetail

INSERT INTO TempAdvCustCommunicationDetail
			(CustomerEntityId
			,RelationEntityId
			--,RelationAddEntityId
			,AddressCategoryAlt_Key
			,AddressTypeAlt_Key
			,Add1
			,Add2
			,Add3
			,CountryAlt_Key
			,CityAlt_Key
			,CityName
			,PinCode
			,RefCustomerId
			,IsMainAddress
			,EffectiveFromTimeKey
			,EffectiveToTimeKey
			--,STD_Code_Res
			,PhoneNo_Res
			--,STD_Code_Off
			,PhoneNo_Off
			,CreatedBy
			,DateCreated
			)

SELECT		A.CustomerEntityId,
			A.RelationEntityId,
			NULL,
			NULL,
			B.PermanentAddress_AddressLine1,
			B.PermanentAddress_AddressLine2,
			B.PermanentAddress_AddressLine3,
			--DC.CountryAlt_Key,
			CAST(NULL AS SMALLINT) AS CountryAlt_Key,
			--DCC.CityAlt_Key,
			CAST(NULL AS SMALLINT) AS CityAlt_Key,
			--RCT.REF_DESC AS CityName,
			B.PermanentAddress_City_Location AS  CityName,
			REPLACE(B.PermanentAddress_Pincode,' ','') AS PinCode,
			A.RefCustomerId AS RefCustomerId,
			'Y' AS IsMainAddress,
			A.EffectiveFromTimeKey
			,A.EffectiveToTimeKey,
			B.PrimaryMobile AS PhoneNo_Res,
			 
			NULL PhoneNo_Off,
			A.CreatedBy,
			A.DateCreated
			
FROM TempAdvCustRelationship  A
INNER JOIN IBL_ENPA_STGDB_V2.dbo.CUSTOMERADDRESS_SOURCESYSTEM_STG B   
on A.RefCustomerId=B.CustomerID




UPDATE TACCMD
   SET TACCMD.RelationAddEntityId= ACCMD.RelationAddEntityId

FROM IBL_ENPA_DB_V2.CURDAT.AdvCustCommunicationDetail ACCMD
Inner Join TempAdvCustCommunicationDetail TACCMD
ON ACCMD.CustomerEntityId=TACCMD.CustomerEntityId
AND ACCMD.IsMainAddress=TACCMD.IsMainAddress
 AND ACCMD.EffectiveToTimeKey=49999

DECLARE @MAX INT
select @MAX=max(RelationAddEntityId)  FROM (SELECT ISNULL(MAX(RelationAddEntityId),0)RelationAddEntityId FROM IBL_ENPA_DB_V2.curdat.AdvCustCommunicationDetail ACCMD
											UNION 
											SELECT ISNULL(MAX(RelationAddEntityId),0)RelationAddEntityId FROM IBL_ENPA_DB_V2.dbo.AdvCustCommunicationDetail_Mod ACCMD)A

UPDATE TempADVCUSTCOMMUNICATIONDETAIL
SET @MAX=RELATIONADDENTITYID=@MAX+1
WHERE RELATIONADDENTITYID IS NULL

UPDATE B SET CityAlt_Key=A.CityAlt_Key
FROM IBL_ENPA_DB_V2.DBO.DIMCITY  A
INNER JOIN TempAdvCustCommunicationDetail B
ON A.CityName=B.CityName
WHERE A.EffectiveToTimeKey=49999

UPDATE TempADVCUSTCOMMUNICATIONDETAIL 
SET COUNTRYALT_KEY=100
WHERE ISNULL(COUNTRYALT_KEY,0)=0

update a set DistrictAlt_Key=b.DistrictAlt_Key
from TempAdvCustCommunicationDetail a
inner join IBL_ENPA_DB_V2.dbo.dimpincode b
on a.pincode=b.pincode
where b.EffectiveToTimeKey=49999

END


GO