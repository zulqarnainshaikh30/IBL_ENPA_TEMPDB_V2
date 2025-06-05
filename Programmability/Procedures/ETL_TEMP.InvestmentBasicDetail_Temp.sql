SET QUOTED_IDENTIFIER, ANSI_NULLS ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [ETL_TEMP].[InvestmentBasicDetail_Temp] 
	-- Add the parameters for the stored procedure here
	
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

    -- Insert statements for procedure here
	

DECLARE  @vEffectivefrom  Int SET @vEffectiveFrom=(SELECT TimeKey FROM UTKS_MISDB.[dbo].Automate_Advances WHERE EXT_FLG='Y')
DECLARE @TimeKey  Int SET @TimeKey=(SELECT TimeKey FROM UTKS_MISDB.[dbo].Automate_Advances WHERE EXT_FLG='Y')
DECLARE @DATE AS DATE =(SELECT Date FROM UTKS_MISDB.[dbo].Automate_Advances WHERE EXT_FLG='Y')

TRUNCATE TABLE UTKS_TEMPDB.dbo.TempInvestmentBasicDetail

INSERT INTO UTKS_TEMPDB.dbo.TempInvestmentBasicDetail --select * from UTKS_TEMPDB.dbo.TempInvestmentBasicDetail
			(BranchCode
			,Inv_Key
			,InvID
			,IssuerID
			,InstrTypeAlt_Key
			,InstrName
			,InvestmentNature
			,InternalRating
			,InRatingDate
			,InRatingExpiryDate
			,ExRating
			,ExRatingAgency
			,ExRatingDate
			,ExRatingExpiryDate
			,Sector
			,Industry_AltKey
			,ListedStkExchange
			,ExposureType
			,SecurityValue
			,MaturityDt
			,ReStructureDate
			,MortgageStatus
			,NHBStatus
			,ResiPurpose
			,AuthorisationStatus
			,EffectiveFromTimeKey
			,EffectiveToTimeKey
			,CreatedBy
			,DateCreated
			,ModifiedBy
			,DateModified
			,ApprovedBy
			,DateApproved
			)

SELECT		A.BranchCode 
			,NULL Inv_Key
			,A.InvID
			,A.IssuerID
			,B.InstrumentTypeAlt_Key
			,A.InstrName
			,A.InvestmentNature
			,NULL InternalRating
			,NULL InRatingDate
			,NULL InRatingExpiryDate
			,NULL ExRating
			,NULL ExRatingAgency
			,NULL ExRatingDate
			,NULL ExRatingExpiryDate
			,A.Sector
			,C.IndustryAlt_Key
			,NULL ListedStkExchange
			,A.ExposureType
			,A.SecurityValue
			,A.MaturityDt
			,A.ReStructureDate
			,NULL MortgageStatus
			,NULL NHBStatus
			,NULL ResiPurpose
			,NULL AuthorisationStatus
			,@vEffectivefrom EffectiveFromTimeKey
			,49999 EffectiveToTimeKey
			,'SSISUSER' CreatedBy
			,GETDATE() DateCreated
			,NULL ModifiedBy
			,NULL DateModified
			,NULL ApprovedBy
			,NULL DateApproved
			
FROM UTKS_STGDB.DBO.TREASURY_INVBASIC_STG  A  
LEFT JOIN UTKS_MISDB.DBO.DimInstrumentType B 
		ON A.InstrTypeCode=B.InstrumentTypeSubGroup AND B.EffectiveFromTimeKey<=@TimeKey AND B.EffectiveToTimeKey>=@TimeKey
LEFT JOIN UTKS_MISDB.DBO.DimIndustry C 
		ON A.IndustryCode=C.SrcSysIndustryCode AND C.EffectiveFromTimeKey<=@TimeKey AND C.EffectiveToTimeKey>=@TimeKey 
INNER JOIN UTKS_TEMPDB.DBO.TempInvestmentIssuerDetail I
		ON I.IssuerID=A.IssuerID

	/* select Timekey from utks_misdb..Automate_Advances where Ext_flg ='Y'
	select *
	FROM UTKS_STGDB.DBO.TREASURY_INVBASIC_STG  A  
LEFT JOIN UTKS_MISDB.DBO.DimInstrumentType B ON A.InstrTypeCode=B.InstrumentTypeSubGroup
AND B.EffectiveFromTimeKey<=26959 AND B.EffectiveToTimeKey>=26959
LEFT JOIN UTKS_MISDB.DBO.DimIndustry C ON A.IndustryCode=C.SrcSysIndustryCode
AND C.EffectiveFromTimeKey<=26959 AND C.EffectiveToTimeKey>=26959 
INNER JOIN UTKS_TEMPDB.DBO.TempInvestmentIssuerDetail I ON I.IssuerID=A.IssuerID
	*/
/*********************************************************************************************************/
/*  Existing Customers Customer Entity ID Update  */ --select * from UTKS_TEMPDB.DBO.TempInvestmentIssuerDetail

UPDATE TEMP 
SET TEMP.InvEntityId=MAIN.InvEntityId
 
FROM  UTKS_TEMPDB.DBO.[TempInvestmentBasicDetail] TEMP
INNER JOIN UTKS_MISDB.[dbo].[InvestmentBasicDetail] MAIN ON TEMP.InvID=MAIN.InvID
WHERE MAIN.EffectiveToTimeKey=49999

--GO
/*********************************************************************************************************/
/*  New Customers Customer Entity ID Update  */
DECLARE @InvEntityId INT=0 
SELECT @InvEntityId=MAX(InvEntityId) FROM  UTKS_MISDB.[dbo].[InvestmentBasicDetail] 
IF @InvEntityId IS NULL  
BEGIN
SET @InvEntityId=0
END
 
UPDATE TEMP 
SET TEMP.InvEntityId=ACCT.InvEntityId
 FROM UTKS_TEMPDB.DBO.[TempInvestmentBasicDetail] TEMP
INNER JOIN (SELECT InvID,(@InvEntityId + ROW_NUMBER()OVER(ORDER BY (SELECT 1))) InvEntityId
			FROM UTKS_TEMPDB.DBO.[TempInvestmentBasicDetail]
			WHERE InvEntityId=0 OR InvEntityId IS NULL)ACCT ON TEMP.InvID=ACCT.InvID

--------------------------------------------------


Update A Set A.IssuerEntityId=B.IssuerEntityId

from UTKS_TEMPDB.DBO.[TempInvestmentBasicDetail] A
Inner JOIN UTKS_TEMPDB.DBO.[TempInvestmentIssuerDetail] B ON A.IssuerId=B.IssuerId



END

GO