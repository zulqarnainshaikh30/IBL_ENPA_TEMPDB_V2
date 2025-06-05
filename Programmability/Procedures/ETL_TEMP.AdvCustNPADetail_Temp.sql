SET QUOTED_IDENTIFIER, ANSI_NULLS ON
GO

-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [ETL_TEMP].[AdvCustNPADetail_Temp] 
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



	TRUNCATe TABLE dbo.TEMPAdvCustNPADetail

	/*INSERT ALL NPA CUSTOMERS DATA IN ADVCUSTNPADETAIL TABLE */	

	INSERT INTO dbo.TempAdvCustNPAdetail
				(
					CustomerEntityId
					,Cust_AssetClassAlt_Key
					,NPADt
					,LastInttChargedDt
					,DbtDt
					,LosDt
					,DefaultReason1Alt_Key
					,DefaultReason2Alt_Key
					,StaffAccountability
					,LastIntBooked
					,RefCustomerID
					,AuthorisationStatus
					,EffectiveFromTimeKey
					,EffectiveToTimeKey
					,CreatedBy
					,DateCreated
					,ModifiedBy
					,DateModified
					,ApprovedBy
					,DateApproved
					,MocStatus
					,MocDate
					,MocTypeAlt_Key
					--,WillfulDefault
					--,WillfulDefaultReasonAlt_Key
					--,WillfulRemark
					--,WillfulDefaultDate
					,NPA_Reason
				)

-------------------------LMS---------------------------
			SELECT 
					B.CustomerEntityId
					,AC.AssetClassAlt_Key Cust_AssetClassAlt_Key
					,AC.NPADATE AS  NPADt
					,NULL LastInttChargedDt
					,NULL DbtDt
					,NULL LosDt
					--,(Case When C.AssetClassSubGroup='DOUBTFUL' then A.DBT_LOS_Date Else Null End) DbtDt
					--,(Case When C.AssetClassSubGroup='LOSS' then A.DBT_LOS_Date Else Null End) LosDt
					,NULL DefaultReason1Alt_Key
					,NULL DefaultReason2Alt_Key
					,NULL StaffAccountability
					,NULL LastIntBooked
					,A.CustomerId RefCustomerID
					,NULL AuthorisationStatus
					,@vEffectivefrom EffectiveFromTimeKey
					,49999 EffectiveToTimeKey
					,'SSISUSER' CreatedBy
					,GETDATE() DateCreated
					,NULL ModifiedBy
					,NULL DateModified
					,NULL ApprovedBy
					,NULL DateApproved
					,NULL MocStatus
					,NULL MocDate
					,NULL MocTypeAlt_Key
					--,NULL WillfulDefault
					--,NULL WillfulDefaultReasonAlt_Key
					--,NULL WillfulRemark
					--,NULL WillfulDefaultDate
					,NULL NPA_Reason

--select * from IBL_ENPA_DB_V2.dbo.DimAssetClassMapping
		FROM IBL_ENPA_STGDB_V2.DBO.CUSTOMER_ALL_SOURCE_SYSTEM A
		    INNER JOIN dbo.TempCustomerBasicDetail B
			on A.CustomerID=B.CustomerId  

		INNER JOIN (SELECT		CustomerID,MAX(AssetClassAlt_Key)AssetClassAlt_Key,MIN(NPADate) NPADate 
					FROM		IBL_ENPA_STGDB_V2.DBO.ACCOUNT_ALL_SOURCE_SYSTEM AC
					INNER JOIN	UTKS_MISDB.dbo.DimAssetClassMapping C
					ON			(C.EffectiveFromTimeKey<=@TimeKey 
					AND			C.EffectiveToTimeKey>=@TimeKey)
					AND			 AC.AssetClassCode=C.SrcSysClassCode -- AssetClassCode
					AND			ISNULL(C.AssetClassShortNameEnum,'STD')<>'STD'
					where		NPADate is not null 
					GROUP BY		CustomerID
					) AC
						ON A.CustomerID =AC.CustomerID
		

/*
UNION 

-----------------OTHER-------------------------------

			SELECT 
					B.CustomerEntityId
					,C.AssetClassAlt_Key Cust_AssetClassAlt_Key
					,A.NPADATE AS  NPADt
					,NULL LastInttChargedDt
					,(Case When C.AssetClassSubGroup='DOUBTFUL' then A.DBT_LOS_Date Else Null End) DbtDt
					,(Case When C.AssetClassSubGroup='LOSS' then A.DBT_LOS_Date Else Null End) LosDt
					,NULL DefaultReason1Alt_Key
					,NULL DefaultReason2Alt_Key
					,NULL StaffAccountability
					,NULL LastIntBooked
					,A.CustomerId RefCustomerID
					,NULL AuthorisationStatus
					,@vEffectivefrom EffectiveFromTimeKey
					,49999 EffectiveToTimeKey
					,'SSISUSER' CreatedBy
					,GETDATE() DateCreated
					,NULL ModifiedBy
					,NULL DateModified
					,NULL ApprovedBy
					,NULL DateApproved
					,NULL MocStatus
					,NULL MocDate
					,NULL MocTypeAlt_Key
					--,NULL WillfulDefault
					--,NULL WillfulDefaultReasonAlt_Key
					--,NULL WillfulRemark
					--,NULL WillfulDefaultDate
					,NULL NPA_Reason
		FROM ENBD_STGDB.DBO.CUSTOMER_SOURCESYSTEM02_STG A
		    INNER JOIN TempCustomerBasicDetail B
			on A.CustomerID=B.CustomerId  
			INNER JOIN IBL_ENPA_DB_V2.dbo.DimAssetClass C
				ON (C.EffectiveFromTimeKey<=@TimeKey AND C.EffectiveToTimeKey>=@TimeKey)
				AND A.AssetClass=C.SrcSysClassCode
				AND ISNULL(C.AssetClassShortNameEnum,'STD')<>'STD' 
 
*/

END


GO