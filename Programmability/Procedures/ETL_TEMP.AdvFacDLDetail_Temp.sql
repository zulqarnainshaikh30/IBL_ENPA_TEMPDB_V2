SET QUOTED_IDENTIFIER, ANSI_NULLS ON
GO

-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [ETL_TEMP].[AdvFacDLDetail_Temp] 
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


TRUNCATE TABLE dbo.TempAdvFacDLDetail
 
INSERT INTO dbo.TEmpAdvFacDLDetail 
		   ([AccountEntityId]
           ,[Principal]
           ,[RepayModeAlt_Key]
           ,[NoOfInstall]
           ,[InstallAmt]
           ,[FstInstallDt]
           ,[LastInstallDt]
           ,[Tenure_Months]
           ,[MarginAmt]
           ,[CommodityAlt_Key]
           ,[RephaseAlt_Key]
           ,[RephaseDt]
           ,[IntServiced]
           ,[SuspendedInterest]
           ,[DerecognisedInterest1]
           ,[DerecognisedInterest2]
           ,[AdjReasonAlt_Key]
           ,[LcNo]
           ,[LcAmt]
           ,[LcIssuingBankAlt_Key]
           ,[ResetFrequency]
           ,[ResetDt]
           ,[Moratorium]
           ,[FirstInstallDtInt]
           ,[ContExcsSinceDt]
           ,[loanPeriod]
           ,[ClaimType]
           ,[ClaimCoverAmt]
           ,[ClaimLodgedDt]
           ,[ClaimLodgedAmt]
           ,[ClaimRecvDt]
           ,[ClaimReceivedAmt]
           ,[ClaimRate]
           ,[RefSystemAcid]
           ,[AuthorisationStatus]
           ,[EffectiveFromTimeKey]
           ,[EffectiveToTimeKey]
           ,[CreatedBy]
           ,[DateCreated]
           ,[ModifiedBy]
           ,[DateModified]
           ,[ApprovedBy]
           ,[DateApproved]
           ,[InstRepaymentDt]
           ,[ScrCrError]
           ,[InttRepaymentDt]
           ,[ScheDuleNo]
           ,[MocStatus]
           ,[MocDate]
           ,[MocTypeAlt_Key]
           ,[UnAppliedIntt]
           ,[NxtInstDay]
           ,[PrplOvduAftrMth]
           ,[PrplOvduAftrDay]
           ,[InttOvduAftrDay]
           ,[InttOvduAftrMth]
           ,[PrinOvduEndMth]
           ,[InttOvduEndMth]
           ,[ScrCrErrorSeq]
           ,[CoverExpiryDt])

		   -------------------LMS ---
SELECT 
	   ACBD.AccountEntityId AS  [AccountEntityId]
           ,A.POSBalance AS [Principal]---SANCTIONAMT lndality      SanctionedAmount  l_loan
           ,NULL AS [RepayModeAlt_Key]   --DIST1FRE  lndality   RepaymentFrequencyID l_loan
           ,NULL AS [NoOfInstall]     ---TRM lndality    RepaymentTerm l_loan
           ,NULL AS [InstallAmt]---PMTPI lndality   InstallmentAmount l_loan
           ,NULL AS [FstInstallDt]---DFP  lndality    InstallmentStartDate l_loan
           ,NULL AS [LastInstallDt]--- MDT lndality   MaturityDate  l_loan
           ,NULL AS [Tenure_Months] --- TRM lndality   RepaymentTerm l_loan
           ,NULL AS [MarginAmt]
           ,NULL AS [CommodityAlt_Key]
           ,NULL AS [RephaseAlt_Key]
           ,NULL AS [RephaseDt]
           ,NULL AS [IntServiced]
           ,NULL AS [SuspendedInterest]
           ,NULL AS [DerecognisedInterest1]
           ,NULL AS [DerecognisedInterest2]
           ,NULL AS [AdjReasonAlt_Key]
           ,NULL AS [LcNo]
           ,NULL AS [LcAmt]
           ,NULL AS [LcIssuingBankAlt_Key]
           ,NULL AS [ResetFrequency]
           ,NULL AS [ResetDt]
           ,NULL AS [Moratorium]
           ,NULL AS [FirstInstallDtInt]
           ,NULL AS [ContExcsSinceDt]
           ,NULL AS [loanPeriod]
           ,NULL AS [ClaimType]
           ,NULL AS [ClaimCoverAmt]
           ,NULL AS [ClaimLodgedDt]
           ,NULL AS [ClaimLodgedAmt]
           ,NULL AS [ClaimRecvDt]
           ,NULL AS [ClaimReceivedAmt]
           ,NULL AS [ClaimRate]
           ,ACBD.SystemAcid AS [RefSystemAcid]
           ,NULL AS [AuthorisationStatus]
           ,@vEffectivefrom [EffectiveFromTimeKey]
           ,49999 [EffectiveToTimeKey]
           ,'SSISUSER' [CreatedBy]
           ,GETDATE() [DateCreated]           
	       ,NULL AS [ModifiedBy]
           ,NULL AS [DateModified]
           ,NULL AS [ApprovedBy]
           ,NULL AS [DateApproved]
           ,NULL AS [InstRepaymentDt]
           ,NULL AS [ScrCrError]
           ,NULL AS [InttRepaymentDt]
           ,NULL AS [ScheDuleNo]
           ,NULL AS [MocStatus]
           ,NULL AS [MocDate]
           ,NULL AS [MocTypeAlt_Key]
           ,NULL AS [UnAppliedIntt]
           ,NULL AS [NxtInstDay]
           ,NULL AS [PrplOvduAftrMth]
           ,NULL AS [PrplOvduAftrDay]
           ,NULL AS [InttOvduAftrDay]
           ,NULL AS [InttOvduAftrMth]
           ,NULL AS [PrinOvduEndMth]
           ,NULL AS [InttOvduEndMth]
           ,NULL AS [ScrCrErrorSeq]
           ,NULL AS [CoverExpiryDt]
		     
FROM dbo.TempAdvAcBasicDetail ACBD
INNER JOIN IBL_ENPA_STGDB_V2.dbo.ACCOUNT_ALL_SOURCE_SYSTEM A ON A.CustomerAcID=ACBD.CustomerACID
WHERE ACBD.FacilityType = 'TL'
/* 
 UNION 

  -------------------OTHER ---
SELECT 
	   ACBD.AccountEntityId AS  [AccountEntityId]
           ,A.POSBalance AS [Principal]---SANCTIONAMT lndality      SanctionedAmount  l_loan
           ,NULL AS [RepayModeAlt_Key]   --DIST1FRE  lndality   RepaymentFrequencyID l_loan
           ,NULL AS [NoOfInstall]     ---TRM lndality    RepaymentTerm l_loan
           ,NULL AS [InstallAmt]---PMTPI lndality   InstallmentAmount l_loan
           ,NULL AS [FstInstallDt]---DFP  lndality    InstallmentStartDate l_loan
           ,NULL AS [LastInstallDt]--- MDT lndality   MaturityDate  l_loan
           ,NULL AS [Tenure_Months] --- TRM lndality   RepaymentTerm l_loan
           ,NULL AS [MarginAmt]
           ,NULL AS [CommodityAlt_Key]
           ,NULL AS [RephaseAlt_Key]
           ,NULL AS [RephaseDt]
           ,NULL AS [IntServiced]
           ,NULL AS [SuspendedInterest]
           ,NULL AS [DerecognisedInterest1]
           ,NULL AS [DerecognisedInterest2]
           ,NULL AS [AdjReasonAlt_Key]
           ,NULL AS [LcNo]
           ,NULL AS [LcAmt]
           ,NULL AS [LcIssuingBankAlt_Key]
           ,NULL AS [ResetFrequency]
           ,NULL AS [ResetDt]
           ,NULL AS [Moratorium]
           ,NULL AS [FirstInstallDtInt]
           ,NULL AS [ContExcsSinceDt]
           ,NULL AS [loanPeriod]
           ,NULL AS [ClaimType]
           ,NULL AS [ClaimCoverAmt]
           ,NULL AS [ClaimLodgedDt]
           ,NULL AS [ClaimLodgedAmt]
           ,NULL AS [ClaimRecvDt]
           ,NULL AS [ClaimReceivedAmt]
           ,NULL AS [ClaimRate]
           ,ACBD.SystemAcid AS [RefSystemAcid]
           ,NULL AS [AuthorisationStatus]
           ,@vEffectivefrom [EffectiveFromTimeKey]
           ,49999 [EffectiveToTimeKey]
           ,'SSISUSER' [CreatedBy]
           ,GETDATE() [DateCreated]           
	       ,NULL AS [ModifiedBy]
           ,NULL AS [DateModified]
           ,NULL AS [ApprovedBy]
           ,NULL AS [DateApproved]
           ,NULL AS [InstRepaymentDt]
           ,NULL AS [ScrCrError]
           ,NULL AS [InttRepaymentDt]
           ,NULL AS [ScheDuleNo]
           ,NULL AS [MocStatus]
           ,NULL AS [MocDate]
           ,NULL AS [MocTypeAlt_Key]
           ,NULL AS [UnAppliedIntt]
           ,NULL AS [NxtInstDay]
           ,NULL AS [PrplOvduAftrMth]
           ,NULL AS [PrplOvduAftrDay]
           ,NULL AS [InttOvduAftrDay]
           ,NULL AS [InttOvduAftrMth]
           ,NULL AS [PrinOvduEndMth]
           ,NULL AS [InttOvduEndMth]
           ,NULL AS [ScrCrErrorSeq]
           ,NULL AS [CoverExpiryDt] 
FROM TempAdvAcBasicDetail ACBD
INNER JOIN IBL_ENPA_STGDB_V2.dbo.ACCOUNT_SOURCESYSTEM02_STG A ON A.CustomerAcID=ACBD.CustomerACID
WHERE ACBD.FacilityType = 'TL'
 
*/


---Remove Duplicate----(Vinit Tamgadge)
;with cta as
(select ROW_NUMBER() over (partition by AccountEntityId order by AccountEntityId) ACID,
* from TEmpAdvFacDLDetail)  
delete from Cta  where ACID>1



END



GO