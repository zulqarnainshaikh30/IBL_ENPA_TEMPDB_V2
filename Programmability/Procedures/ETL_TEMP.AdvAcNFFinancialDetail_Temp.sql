SET QUOTED_IDENTIFIER, ANSI_NULLS ON
GO
CREATE PROCEDURE [ETL_TEMP].[AdvAcNFFinancialDetail_Temp]
AS
BEGIN

TRUNCATE TABLE dbo.[TempAdvNFAcFinancialDetail]

DECLARE  @vEffectivefrom  Int SET @vEffectiveFrom=(SELECT TimeKey FROM IBL_ENPA_DB_V2.[dbo].SYSDATAMATRIX WHERE CurrentStatus='C')
DECLARE @TimeKey  Int SET @TimeKey=(SELECT TimeKey FROM IBL_ENPA_DB_V2.[dbo].SYSDATAMATRIX WHERE CurrentStatus='C')
DECLARE @DATE AS DATE =(SELECT Date FROM IBL_ENPA_DB_V2.[dbo].SYSDATAMATRIX WHERE CurrentStatus='C')
--------------------------------------------------------------------------------------------------------------------------------------------------
--SET IDENTITY_INSERT dbo.TempAdvNFAcFinancialDetail OFF
INSERT INTO dbo.TempAdvNFAcFinancialDetail
(	
--CustomerEntityId,
--AccountEntityId,
Ac_ReviewTypeAlt_key
,Ac_ReviewAuthAlt_Key
,Ac_NextReviewDueDt
,DrawingPower
,InttRate
,NpaDt
,BalanceInCurrency
,Balance
,SignBalance
,OverDue
,UnDrawnAmt
,ProvSecured
,ProvUnSecured
,AdditionalProv
,TotalProv
,SecTangAst
,CoverGovGur
,Unsecured
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
,MocDate
,MocStatus
,MocTypeAlt_Key
,Ac_ReviewAuthLevelAlt_Key

)
--------------------------------FINACLE-----------------------------------
	SELECT	
			--ACBD.CustomerEntityId as CustomerEntityId,
			--ACBD.AccountEntityId AS  AccountEntityId,
			0 AS Ac_ReviewTypeAlt_key
			,0 AS Ac_ReviewAuthAlt_Key
			,Review_RenewDueDt AS  Ac_NextReviewDueDt
			,DrawingPower AS DrawingPower   --WDLMT  lnddalyt
			,A.InttRate AS InttRate
			,A.NPADate AS NpaDt
			,BalanceInCurrency
			,Balance
			,SignBalance
			,OverDue
			,NULL UnDrawnAmt
			,NULL ProvSecured
			,NULL ProvUnSecured
			,NULL AdditionalProv
			,TotalProv
			,NULL SecTangAst
			,NULL CoverGovGur
			,NULL Unsecured			
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
			,ACBD.MocDate
			,ACBD.MocStatus
			,ACBD.MocTypeAlt_Key
			,NULL Ac_ReviewAuthLevelAlt_Key			
FROM IBL_ENPA_STGDB_V2.DBO.ACCOUNT_ALL_SOURCE_SYSTEM A
			left JOIN IBL_ENPA_DB_V2.DBO.DIMSOURCEDB DS ON DS.SourceName=A.SourceSystem
			AND DS.EffectiveFromTimeKey<=@TimeKey And DS.EffectiveToTimeKey>=@TimeKey
			LEFT JOIN TempAdvNFAcBasicDetail ACBD ON A.CustomerAcID=ACBD.CustomerACID ---And DS.SourceAlt_Key=ACBD.SourceAlt_Key
			LEFT JOIN  IBL_ENPA_TEMPDB_V2.DBO.TempAdVAcBalanceDetail AABD  on AABD.AccountEntityId=ACBD.AccountEntityId
			LEFT JOIN IBL_ENPA_DB_V2.DBO.DimProduct DP
				ON  DP.EffectiveFromTimeKey<=@TimeKey 
				And DP.EffectiveToTimeKey>=@TimeKey
				AND DP.PRODUCTALT_KEY=ACBD.ProductAlt_Key


--------------------------------------------------------------------------------------------------------------------------------------------------


	--UPDATE A
	--	SET Ac_NextReviewDueDt=NULL
	--FROM  TempAdvNFAcFinancialDetail A
	--	INNER JOIN TempAdvNFAcBasicDetail B
	--		ON A.AccountEntityId=B.AccountEntityId
	--	INNER JOIN IBL_ENPA_DB_V2.DBO.DimProduct C
	--		ON C.EffectiveFromTimeKey<=@TimeKey AND C.EffectiveToTimeKey>=@TimeKey
	--		AND B.ProductAlt_Key=C.ProductAlt_Key
	--	 WHERE ISNULL(C.REVIEWFLAG,'N')='N'

END
GO