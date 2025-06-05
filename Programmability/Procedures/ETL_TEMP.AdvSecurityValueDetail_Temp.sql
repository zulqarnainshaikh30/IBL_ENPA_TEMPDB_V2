SET QUOTED_IDENTIFIER, ANSI_NULLS ON
GO

-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [ETL_TEMP].[AdvSecurityValueDetail_Temp] 
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

  TRUNCATE TABLE TempAdvSecurityValueDetail

  INSERT INTO TempAdvSecurityValueDetail
           (SecurityEntityID
			,ValuationSourceAlt_Key
			,ValuationDate
			,CurrentValue
			,ValuationExpiryDate
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
			)

   -------------    LMS  ---------
   SELECT   D.SecurityEntityID
			,NULL ValuationSourceAlt_Key
			,A.ValuationDate
			,A.SecurityValue CurrentValue
			,A.ValuationExpiryDate
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

	
			FROM	IBL_ENPA_STGDB_V2.dbo.Security_All_Source_System A  
 INNER JOIN			TempCustomerBasicDetail B ON cast(A.CustomerID as int)=B.Customerid  
     AND			B.EffectiveFromTimeKey<=@TimeKey AND B.EffectiveToTimeKey>=@TimeKey  
 LEFT JOIN			IBL_ENPA_DB_V2.[dbo].[DimCollateralSubType] C ON C.SrcSecurityCode=Isnull(A.SecurityCODE,'Others') --SecurityType
     AND			C.EffectiveFromTimeKey<=@TimeKey AND C.EffectiveToTimeKey>=@TimeKey 
	 				--AND B.EffectiveFromTimeKey<=@TimeKey AND B.EffectiveToTimeKey>=@TimeKey
	INNER JOIN		TempAdvSecurityDetail D ON D.RefCustomerId=B.CustomerID 
	and				A.Security_ID = D.Security_RefNo and A.AccountID = D.RefSystemAcId
					--AND C.EffectiveFromTimeKey<=@TimeKey AND C.EffectiveToTimeKey>=@TimeKey 	
  WHERE				C.Valid='Y'
			
	
			---Update By Vinit--
		--;with CTE As(
		--select Row_number() over (partition by SecurityEntityID order by SecurityEntityID) SEID ,* from TempAdvSecurityValueDetail)
		--delete from CTE   where SEID>1

END

--select * from TempAdvSecurityValueDetail  

GO