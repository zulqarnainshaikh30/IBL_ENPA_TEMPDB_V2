SET QUOTED_IDENTIFIER, ANSI_NULLS ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [ETL_TEMP].[AdvAcOtherDetail_Temp] 
	-- Add the parameters for the stored procedure here
	
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

    -- Insert statements for procedure here
	
TRUNCATE TABLE dbo.TempAdvAcOtherDetail

INSERT INTO dbo.TempAdvAcOtherDetail
			(AccountEntityId
			,GovGurAmt
			,SplCatg1Alt_Key
			,SplCatg2Alt_Key
			,RefinanceAgencyAlt_Key
			,RefinanceAmount
			,BankAlt_Key
			,TransferAmt
			,ProjectId
			,ConsortiumId
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
			,MocStatus
			,MocDate
			,SplCatg3Alt_Key
			,SplCatg4Alt_Key
			,MocTypeAlt_Key
			,GovGurExpDt
			)

SELECT		A.AccountEntityId
			,NULL GovGurAmt
			,NULL SplCatg1Alt_Key
			,NULL SplCatg2Alt_Key
			,NULL RefinanceAgencyAlt_Key
			,NULL RefinanceAmount
			,NULL BankAlt_Key
			,NULL TransferAmt
			,NULL ProjectId
			,NULL ConsortiumId
			,A.SystemACID RefSystemAcId
			,NULL AuthorisationStatus
			,A.EffectiveFromTimeKey EffectiveFromTimeKey
			,A.EffectiveToTimeKey EffectiveToTimeKey
			,A.CreatedBy CreatedBy
			,A.DateCreated DateCreated
			,NULL ModifiedBy
			,NULL DateModified
			,NULL ApprovedBy
			,NULL DateApproved
			,NULL D2Ktimestamp
			,NULL MocStatus
			,NULL MocDate
			,NULL SplCatg3Alt_Key
			,NULL SplCatg4Alt_Key
			,NULL MocTypeAlt_Key
			,NULL GovGurExpDt
			
FROM dbo.TempAdvAcBasicDetail  A



END


GO