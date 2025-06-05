SET QUOTED_IDENTIFIER, ANSI_NULLS ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [ETL_TEMP].[InvestmentIssuerDetail_Temp] 
	-- Add the parameters for the stored procedure here
	
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

    -- Insert statements for procedure here
	

DECLARE  @vEffectivefrom  Int =(SELECT TimeKey FROM IBL_ENPA_DB_V2.[dbo].Automate_Advances WHERE EXT_FLG='Y')
DECLARE @TimeKey  Int =(SELECT TimeKey FROM IBL_ENPA_DB_V2.[dbo].Automate_Advances WHERE EXT_FLG='Y')
DECLARE @DATE AS DATE =(SELECT Date FROM IBL_ENPA_DB_V2.[dbo].Automate_Advances WHERE EXT_FLG='Y')

TRUNCATE TABLE IBL_ENPA_TEMPDB_V2.dbo.TempInvestmentIssuerDetail
--DECLARE  @vEffectivefrom  Int SET @vEffectiveFrom=(SELECT TimeKey FROM IBL_ENPA_DB_V2.[dbo].Automate_Advances WHERE EXT_FLG='Y')
INSERT INTO IBL_ENPA_TEMPDB_V2.dbo.TempInvestmentIssuerDetail
			(
			BranchCode
			,Issuer_Key
			,IssuerAltkey
			,IssuerID
			,IssuerName
			,RatingStatus
			,IssuerAccpRating
			,IssuerAccpRatingDt
			,IssuerRatingAgency
			,Ref_Txn_Sys_Cust_ID
			,Issuer_Category_Code
			,GrpEntityOfBank
			,AuthorisationStatus
			,EffectiveFromTimeKey
			,EffectiveToTimeKey
			,CreatedBy
			,DateCreated
			,ModifiedBy
			,DateModified
			,ApprovedBy
			,DateApproved
			,SourceAlt_key
			,UcifId
			--,PanNo
			
			)
--DECLARE  @vEffectivefrom  Int SET @vEffectiveFrom=(SELECT TimeKey FROM IBL_ENPA_DB_V2.[dbo].Automate_Advances WHERE EXT_FLG='Y')
SELECT		A.BranchCode
			,NULL Issuer_Key
			,NULL IssuerAltkey
			,A.IssuerID
			,A.IssuerName
			,NULL RatingStatus
			,NULL IssuerAccpRating
			,NULL IssuerAccpRatingDt
			,NULL IssuerRatingAgency
			,A.Ref_Txn_Sys_Cust_ID
			,A.Issuer_Category_Code
			,A.GrpEntityOfBank
			,NULL AuthorisationStatus
			,@vEffectivefrom EffectiveFromTimeKey
			,49999 EffectiveToTimeKey
			,'SSISUSER' CreatedBy
			,GETDATE() DateCreated
			,NULL ModifiedBy
			,NULL DateModified
			,NULL ApprovedBy
			,NULL DateApproved
			,S.SourceAlt_Key SourceAlt_key
			,UCIC_ID UcifId
FROM	IBL_ENPA_STGDB_V2.DBO.TREASURY_INVISSUER_STG  A  
LEFT JOIN		IBL_ENPA_DB_V2.[dbo].DIMSOURCEDB S ON S.SourceName=A.SourceSystem
AND				S.EffectiveFromTimeKey<=@TimeKey AND S.EffectiveToTimeKey>=@TimeKey


/*********************************************************************************************************/
/*  Existing Customers Customer Entity ID Update  */

UPDATE TEMP 
SET TEMP.IssuerEntityId=MAIN.IssuerEntityId
 
FROM  IBL_ENPA_TEMPDB_V2.DBO.[TempInvestmentIssuerDetail] TEMP
INNER JOIN IBL_ENPA_DB_V2.[dbo].[InvestmentIssuerDetail] MAIN ON TEMP.IssuerID=MAIN.IssuerID
WHERE MAIN.EffectiveToTimeKey=49999

--GO
/*********************************************************************************************************/
/*  New Customers Customer Entity ID Update  */
DECLARE @IssuerEntityId INT=0 
SELECT @IssuerEntityId=MAX(IssuerEntityId) FROM  IBL_ENPA_DB_V2.[dbo].[InvestmentIssuerDetail] 
IF @IssuerEntityId IS NULL  
BEGIN
SET @IssuerEntityId=0
END
 
UPDATE TEMP 
SET TEMP.IssuerEntityId=ACCT.IssuerEntityId
 FROM IBL_ENPA_TEMPDB_V2.DBO.[TempInvestmentIssuerDetail] TEMP
INNER JOIN (SELECT IssuerID,(@IssuerEntityId + ROW_NUMBER()OVER(ORDER BY (SELECT 1))) IssuerEntityId
			FROM IBL_ENPA_TEMPDB_V2.DBO.[TempInvestmentIssuerDetail]
			WHERE IssuerEntityId=0 OR IssuerEntityId IS NULL)ACCT ON TEMP.IssuerID=ACCT.IssuerID

--------------------------------------------------


END


--select * from  IBL_ENPA_STGDB_V2.[dbo].[TREASURY_INVISSUER_STG]
GO