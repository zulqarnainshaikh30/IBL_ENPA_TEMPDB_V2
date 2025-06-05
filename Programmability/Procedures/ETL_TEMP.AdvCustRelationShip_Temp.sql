SET QUOTED_IDENTIFIER, ANSI_NULLS ON
GO

-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [ETL_TEMP].[AdvCustRelationShip_Temp] 
	-- Add the parameters for the stored procedure here
	
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

    -- Insert statements for procedure here
	TRUNCATE TABLE TempAdvCustRelationship

DECLARE  @vEffectivefrom  Int SET @vEffectiveFrom=(SELECT TimeKey FROM IBL_ENPA_DB_V2.[dbo].Automate_Advances WHERE EXT_FLG='Y')
DECLARE @TimeKey  Int SET @TimeKey=(SELECT TimeKey FROM IBL_ENPA_DB_V2.[dbo].Automate_Advances WHERE EXT_FLG='Y')
DECLARE @DATE AS DATE =(SELECT Date FROM IBL_ENPA_DB_V2.[dbo].Automate_Advances WHERE EXT_FLG='Y')

Select * from TempAdvCustRelationship
 
INSERT INTO TempAdvCustRelationship
		   (CustomerEntityId,
			RelationEntityId,
			SalutationAlt_Key,
			Name,
			ConstitutionAlt_Key,
			OccupationAlt_Key,
			ReligionAlt_Key,
			CasteAlt_Key,
			FarmerCatAlt_Key,
			MaritalStatusAlt_Key,
			NetWorth,
			DateofBirth,
			Qualification1Alt_Key,
			Qualification2Alt_Key,
			Qualification3Alt_Key,
			Qualification4Alt_Key,
			MobileNo,
			Email,
			VoterID,
			RationCardNo,
			AadhaarId,
			NPR_Id,
			PassportNo,
			PassportIssueDt,
			PassportExpiryDt,
			PassportIssueLocation,
			DL_No,
			DL_IssueDate,
			DL_ExpiryDate,
			DL_IssueLocation,
			BusiEntity_NationalityTypeAlt_Key,
			NationalityCountryAlt_Key,
			PAN,
			TAN,
			TIN,
			RegistrationNo,
			DIN,
			CIN,
			ServiceTax,
			OtherID,
			OtherIdType,
			RegistrationAuth,
			RegistrationAuthLocation,
			PrevFinYearSales,
			EmployeeCount,
			SalesFigFinYr,
			Designation_ContactPeroson,
			IncorporationDate,
			BusinessCategoryAlt_Key,
			BusinessIndustryTypeAlt_Key,
			SharePercent,
			RetirementDate,
			ProfessionArea,
			ExistingCustomer,
			RefCustomerId,
			EffectiveFromTimeKey,
			EffectiveToTimeKey,
			CreatedBy,
			DateCreated,
			CIBILPGId
			,LEI)
			 
SELECT		TCBD.CustomerEntityId, 
			RelationEntityId As RelationEntityId, 
			TCBD.CustSalutationAlt_Key, 
			TCBD.customername, 
			TCBD.ConstitutionAlt_Key, 
			TCBD.OccupationAlt_Key, 
			TCBD.ReligionAlt_Key, 
			TCBD.CasteAlt_Key, 
			TCBD.FarmerCatAlt_Key, 
			TCBD.MaritalStatusAlt_Key, 
			NULL As NetWorth, 
			NULL As DateofBirth, 
			NULL As Qualification1Alt_Key, 
			NULL As Qualification2Alt_Key, 
			NULL As Qualification3Alt_Key, 
			NULL As Qualification4Alt_Key, 
			NULL As MobileNo, 
			NULL As Email, 
			NULL As VoterID, 
			NULL As RationCardNo, 
			NULL As AadhaarId, 
			NULL As NPR_Id, 
			NULL As PassportNo, 
			NULL As PassportIssueDt, 
			NULL As PassportExpiryDt, 
			NULL As PassportIssueLocation, 
			NULL As DL_No, 
			NULL As DL_IssueDate, 
			NULL As DL_ExpiryDate, 
			NULL As DL_IssueLocation, 
			NULL As BusiEntity_NationalityTypeAlt_Key, 
			NULL As NationalityCountryAlt_Key, 
			stga.PANNO as PAN, 
			NULL As TAN, 
			NULL As TIN, 
			NULL As RegistrationNo, 
			NULL As DIN, 
			NULL As CIN, 
			NULL As ServiceTax, 
			NULL As OtherID, 
			NULL As OtherIdType, 
			NULL As RegistrationAuth, 
			NULL As RegistrationAuthLocation, 
			NULL As PrevFinYearSales, 
			NULL As EmployeeCount, 
			NULL As SalesFigFinYr, 
			NULL As Designation_ContactPeroson, 
			NULL As IncorporationDate, 
			NULL As BusinessCategoryAlt_Key, 
			NULL As BusinessIndustryTypeAlt_Key, 
			NULL As SharePercent, 
			NULL As RetirementDate, 
			NULL As ProfessionArea, 
			(CASE WHEN TCBD.CustomerEntityId IS NOT NULL THEN 'Y' 
			      ELSE 'N' END) As ExistingCustomer, 
			TAAR.CustomerId, 
	     	@vEffectivefrom,
			49999,
			'SSISUSER',
			Convert(Date, Getdate(),103), 
			NULL As CIBILPGId 
			,NULL AS LEI
FROM (SELECT DISTINCT RefCustomerID As CustomerId,RelationEntityId from TempAdvAcRelations) TAAR
Inner JOIN TempCustomerBasicDetail TCBD on Convert(int,TAAR.CustomerId) = convert(int,TCBD.CustomerID)
left join IBL_ENPA_STGDB_V2.DBO.CUSTOMER_ALL_SOURCE_SYSTEM stga on convert(int,stga.CustomerId)=Convert(int,TCBD.CustomerID) ----Newly added line on date 21/02/2024


--INNER JOIN IBL_ENPA_STGDB_V2.DBO.CUSTOMERRELATION_SOURCESYSTEM_STG CR ON CR.CustomerID=TAAR.CustomerID  ------Previously commented  on date 21/02/2024
--LEFT JOIN IBL_ENPA_STGDB_V2.DBO.CUSTOMERADDRESS_SOURCESYSTEM_STG CA ON CA.CustomerID=CR.CustomerID

 


---==============UPDATE  CUSTOMERNAME WHERE NAME IS BLANK---==============


  UPDATE A
  SET  NAME=CUSTOMERNAME
    
  FROM  TempADVCUSTRELATIONSHIP  A 
  INNER JOIN TempCUSTOMERBASICDETAIL B  
  ON A.REFCUSTOMERID=B.CUSTOMERID
  WHERE NAME IS NULL

  
END


GO