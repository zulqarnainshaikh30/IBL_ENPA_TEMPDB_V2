SET QUOTED_IDENTIFIER, ANSI_NULLS ON
GO

  
  
CREATE proc [ETL_TEMP].[TEMPAdvFacNFDetail]  
AS  

DECLARE  @vEffectivefrom  Int SET @vEffectiveFrom=(SELECT TimeKey FROM IBL_ENPA_DB_V2.[dbo].SYSDATAMATRIX WHERE CurrentStatus='C')
DECLARE @TimeKey  Int SET @TimeKey=(SELECT TimeKey FROM IBL_ENPA_DB_V2.[dbo].SYSDATAMATRIX WHERE CurrentStatus='C')
DECLARE @DATE AS DATE =(SELECT Date FROM IBL_ENPA_DB_V2.[dbo].SYSDATAMATRIX WHERE CurrentStatus='C')

Truncate table [IBL_ENPA_TEMPDB_V2].[DBO].[TEMPAdvFacNFDetail]

INSERT INTO [DBO].[TEMPAdvFacNFDetail]
(
AccountEntityId
,D2KFacilityID
,GLAlt_key
,Operative_Acid
,LCBG_TYPE
,LCBGNo
,LcBgAmt
,OriginDt
,EffectiveDt
,ExpiryDt
,ExtensionDt
,TypeAlt_Key
,NatureAlt_Key
,BeneficiaryType
,BeneficiaryName
,Balance
,BalanceInCurrency
,CurrencyAlt_Key
,CountryAlt_Key
,NegotiatingBank
,MarginType
,MarginAmt
,PurposeAlt_Key
,ShipmentDt
,CoveredByBank
,CoveredByBankAlt_Key
,InvocationDt
,Commission
,BillReceived
,BillsUnderCollAmt
,FundedConversionDt
,Datepaid
,RecoveryDt
,CounterGuar
,CorresBankCode
,CorresBrCode
,ClaimDt
,NFFacilityNo
,Periodicity
,CommissionDue
,DueDateOfRecovery
,CommOnDuedateYN
,DelayReason
,PresentPosition
,AmmountRecovered
,ScrCrError
,AdjDt
,AdjReasonAlt_Key
,EntityClosureDate
,EntityClosureReasonAlt_Key
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
,MocStatus
,MocDate
,MocTypeAlt_Key
,GovtGurantee
,GovGurAmt
,ScrCrErrorSeq
,ApplicationDt
,ClaimExpiryDt
,InvocationStatusAlt_Key
,provision
,MarginAccNo
)

select 
ACBD.AccountEntityId
,NULL as D2KFacilityID
,B.GLAlt_key
,NULL Operative_Acid
,NULL as LCBG_TYPE
,ACBD.AccountEntityId as LCBGNo
,NULL as LcBgAmt
,NULL as OriginDt
,NULL as EffectiveDt
,NULL as ExpiryDt
,NULL as ExtensionDt
,0 as TypeAlt_Key
,0 as NatureAlt_Key
,NULL as BeneficiaryType
,NULL as BeneficiaryName
,case when isnull(BalanceOutstandingINR,0)<=0 then isnull(BalanceOutstandingINR,0)*-1 else 0 end AS BalanceOutstandingINR
,BalanceInActualAcCurrency
,ACBD.CurrencyAlt_Key
,0 as CountryAlt_Key
,NULL as NegotiatingBank
,NULL as MarginType
,NULL as MarginAmt
,0 as PurposeAlt_Key
,NULL as ShipmentDt
,NULL as CoveredByBank
,0 as CoveredByBankAlt_Key
,NULL as InvocationDt
,NULL as Commission
,NULL as BillReceived
,NULL as BillsUnderCollAmt
,NULL as FundedConversionDt
,NULL as Datepaid
,NULL as RecoveryDt
,NULL as CounterGuar
,NULL as CorresBankCode
,NULL as CorresBrCode
,NULL as ClaimDt
,NULL as NFFacilityNo
,NULL as Periodicity
,NULL as CommissionDue
,NULL as DueDateOfRecovery
,NULL as CommOnDuedateYN
,NULL as DelayReason
,NULL as PresentPosition
,NULL as AmmountRecovered
,NULL as ScrCrError
,NULL as AdjDt
,0 as AdjReasonAlt_Key
,NULL as EntityClosureDate
,0 as EntityClosureReasonAlt_Key
,ACBD.RefCustomerId [RefCustomerId]
,ACBD.SystemACID [RefSystemAcId]
,NULL AS AuthorisationStatus
,@vEffectivefrom AS EffectiveFromTimeKey
,49999 AS EffectiveToTimeKey
,'SSISUSER' AS CreatedBy
,GETDATE() AS DateCreated
			,NULL AS ModifiedBy
			,NULL AS DateModified
			,NULL AS ApprovedBy
			,NULL AS DateApproved
			,NULL AS D2Ktimestamp
			,ACBD.MocStatus
			,ACBD.MocDate
			,ACBD.MocTypeAlt_Key
			,NULL as GovtGurantee
			,NULL as GovGurAmt
			,NULL as ScrCrErrorSeq
			,NULL as ApplicationDt
			,NULL as ClaimExpiryDt
			,0 as InvocationStatusAlt_Key
			,NULL as provision
			,NULL as MarginAccNo
FROM				IBL_ENPA_STGDB_V2.[dbo].ACCOUNT_ALL_SOURCE_SYSTEM A
	LEFT JOIN		IBL_ENPA_DB_V2.[dbo].[DimGL] B ON A.GLCode=B.GLCode
					AND B.EffectiveFromTimeKey<=@TimeKey AND B.EffectiveToTimeKey>=@TimeKey
	LEFT JOIN		IBL_ENPA_DB_V2.[dbo].[DimProduct] C 
					ON C.ProductCode=A.Scheme_ProductCode		
					AND C.EffectiveFromTimeKey<=@TimeKey AND C.EffectiveToTimeKey>=@TimeKey
	LEFT JOIN		IBL_ENPA_DB_V2.[dbo].[DimActivity] D ON D.SrcSysActivityCode=A.[PurposeofAdvance]
					AND D.EffectiveFromTimeKey<=@TimeKey AND D.EffectiveToTimeKey>=@TimeKey
	LEFT JOIN		IBL_ENPA_DB_V2.[dbo].[DimSubSector] E ON E.SubSectorName=A.[Sector]
					AND E.EffectiveFromTimeKey<=@TimeKey AND E.EffectiveToTimeKey>=@TimeKey
	LEFT JOIN		IBL_ENPA_DB_V2.[dbo].[DimIndustry] F ON F.SrcSysIndustryCode=A.IndustryCode
					AND F.EffectiveFromTimeKey<=@TimeKey AND F.EffectiveToTimeKey>=@TimeKey
	LEFT JOIN		IBL_ENPA_DB_V2.[dbo].[DimCurrency] G ON G.SrcSysCurrencyCode=A.CurrencyCode
					AND G.EffectiveFromTimeKey<=@TimeKey AND G.EffectiveToTimeKey>=@TimeKey
	--LEFT JOIN		IBL_ENPA_DB_V2.[dbo].[DimCountry] H ON H.CountryShortNameEnum=A.CurrencyCode
	--				AND H.EffectiveFromTimeKey<=@TimeKey AND H.EffectiveToTimeKey>=@TimeKey
	LEFT JOIN		IBL_ENPA_DB_V2.[dbo].DIMSOURCEDB S ON S.SourceName=A.SourceSystem
					AND S.EffectiveFromTimeKey<=@TimeKey AND S.EffectiveToTimeKey>=@TimeKey
INNER JOIN			TempAdvNFAcBasicDetail ACBD		ON A.CustomerAcID=ACBD.CustomerACID
INNER JOIN			IBL_ENPA_STGDB_V2.dbo.CUSTAC_MERGE_INCREMENTAL I ON A.CustomerAcID=I.CustomerAcID
													AND ISNULL(I.Funded_NonFunded_Flag,'')<>'Y'

	WHERE			C.FacilityType IN ('NF','LC','BG')



GO