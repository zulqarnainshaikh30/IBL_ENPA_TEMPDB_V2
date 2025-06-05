SET QUOTED_IDENTIFIER, ANSI_NULLS ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [ETL_TEMP].[AdvCustOtherDetail_Temp] 
	-- Add the parameters for the stored procedure here
	
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

    -- Insert statements for procedure here
	
TRUNCATE TABLE dbo.TempAdvCustOtherDetail

INSERT INTO dbo.TempAdvCustOtherDetail
			(CustomerEntityId
			,OrgCostOfEquip
			,OrgCostOfPlantMech
			,DepValPlant
			,ValLand
			,IECDno
			,GroupAlt_Key
			,CustomerSwiftCode
			,SplCatg1Alt_Key
			,SplCatg2Alt_Key
			,SplCatg3Alt_Key
			,SplCatg4Alt_Key
			,CmaEligible
			,PFNo
			,SupperAnnuationBenefit
			,SupperannuationBenefitValuationDt
			,BusinessCommenceDt
			,CancelObtained
			,TotConsortiumLimitFunded
			,TotConsortiumLimitNonFunded
			,UpgradationDate
			,CustomerExpiredYN
			,TotWCLimitFunded
			,Flagged_SubSector
			,RefCustomerId
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
			,MocTypeAlt_Key
			,AnnualExportTurnover
			,FMCNumber
			,IsEmployee
			,IsPetitioner
			,UnderLitigation
			,PermiNatureID
			,BorrUnitFunct
			,DtofClosure
			,NonCoopBorrower
			,ArbiAgreement
			,TransThroughUs
			,CutBackArrangement
			,BankingArrangement
			,MemberBanksNo
			,TotalConsortiumAmt
			,ROC_CFCReportDate
			,ROC_ChargeFV
			,ROC_ChargeFVDt
			,ROC_ChargeRemark
			,ROC_Securities
			,ROC_Cover
			,ROC_CoveredDt
			,ChargeFiledWith
			,FiledDt
			,EmployeeID
			,EmployeeType
			,Designation
			,Placeofposting
			,LPersonalConDate
			,LPersonalConDtls
			,RecallNoticeDate
			,RecallNoticeModeID
			,LegalAuditDate
			,IrregularityPending
			,IrregularityRectiDate
			,FraudAccoStatus
			,PreSARFAESINoticeDt
			,FMRNO
			,FMRDate
			,GradeScaleAlt_Key
			,FraudNatureRemark
			,ROCCoveredCertificateRemark
			,ReasonsNonCoOperativeBorrower
			,StatusNonCoOperativeBorrower
			)

SELECT		A.CustomerEntityId
			,NULL OrgCostOfEquip
			,NULL OrgCostOfPlantMech
			,NULL DepValPlant
			,NULL ValLand
			,NULL IECDno
			,NULL GroupAlt_Key
			,NULL CustomerSwiftCode
			,NULL SplCatg1Alt_Key
			,NULL SplCatg2Alt_Key
			,NULL SplCatg3Alt_Key
			,NULL SplCatg4Alt_Key
			,NULL CmaEligible
			,NULL PFNo
			,NULL SupperAnnuationBenefit
			,NULL SupperannuationBenefitValuationDt
			,NULL BusinessCommenceDt
			,NULL CancelObtained
			,NULL TotConsortiumLimitFunded
			,NULL TotConsortiumLimitNonFunded
			,NULL UpgradationDate
			,NULL CustomerExpiredYN
			,NULL TotWCLimitFunded
			,NULL Flagged_SubSector
			,A.CustomerId RefCustomerId
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
			,NULL MocTypeAlt_Key
			,NULL AnnualExportTurnover
			,NULL FMCNumber
			,NULL IsEmployee
			,NULL IsPetitioner
			,NULL UnderLitigation
			,NULL PermiNatureID
			,NULL BorrUnitFunct
			,NULL DtofClosure
			,NULL NonCoopBorrower
			,NULL ArbiAgreement
			,NULL TransThroughUs
			,NULL CutBackArrangement
			,NULL BankingArrangement
			,NULL MemberBanksNo
			,NULL TotalConsortiumAmt
			,NULL ROC_CFCReportDate
			,NULL ROC_ChargeFV
			,NULL ROC_ChargeFVDt
			,NULL ROC_ChargeRemark
			,NULL ROC_Securities
			,NULL ROC_Cover
			,NULL ROC_CoveredDt
			,NULL ChargeFiledWith
			,NULL FiledDt
			,NULL EmployeeID
			,NULL EmployeeType
			,NULL Designation
			,NULL Placeofposting
			,NULL LPersonalConDate
			,NULL LPersonalConDtls
			,NULL RecallNoticeDate
			,NULL RecallNoticeModeID
			,NULL LegalAuditDate
			,NULL IrregularityPending
			,NULL IrregularityRectiDate
			,NULL FraudAccoStatus
			,NULL PreSARFAESINoticeDt
			,NULL FMRNO
			,NULL FMRDate
			,NULL GradeScaleAlt_Key
			,NULL FraudNatureRemark
			,NULL ROCCoveredCertificateRemark
			,NULL ReasonsNonCoOperativeBorrower
			,NULL StatusNonCoOperativeBorrower
			
			
FROM dbo.TempCustomerBasicDetail  A

;with cte as 
(select *,
	ROW_NUMBER() OVER (PARTITION BY CustomerEntityid ORDER BY CustomerEntityid) as Rnk 
	from dbo.TempAdvCustOtherDetail
) delete from cte where Rnk > 1

END


GO