SET QUOTED_IDENTIFIER, ANSI_NULLS ON
GO
 
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [ETL_TEMP].[ExceptionFinalStatusType_Temp] 
	-- Add the parameters for the stored procedure here
	
AS
BEGIN
	SET NOCOUNT ON;
	


		DECLARE  @vEffectivefrom  Int SET @vEffectiveFrom=(SELECT TimeKey FROM IBL_ENPA_DB_V2.[dbo].Automate_Advances WHERE EXT_FLG='Y')
		DECLARE @TimeKey  Int SET @TimeKey=(SELECT TimeKey FROM IBL_ENPA_DB_V2.[dbo].Automate_Advances WHERE EXT_FLG='Y')
		DECLARE @DATE AS DATE =(SELECT Date FROM IBL_ENPA_DB_V2.[dbo].Automate_Advances WHERE EXT_FLG='Y')


		
		----Declare @Date date = (select Date from Automate_Advances where Ext_flg = 'Y')

		----Delete from ExceptionFinalStatusType where EffectiveFromTimeKey = @Timekey  and EffectiveToTimeKey >= @Timekey and IS_ETL = 'Y' and  StatusType = 'TWO'
		TRUNCATE TABLE IBL_ENPA_TEMPDB_V2.dbo.TempExceptionFinalStatusType

		INSERT INTO IBL_ENPA_TEMPDB_V2.dbo.TempExceptionFinalStatusType
			(SourceAlt_Key,CustomerID,ACID,StatusType,StatusDate,Amount,EffectiveFromTimeKey,EffectiveToTimeKey,IS_ETL)

		select B.SourceAlt_Key,cast(A.customerid as varchar(50)) + '_' + cast(B.SourceAlt_key as varchar(50)),
		A.CustomerAcID,'TWO',ISNULL(TWODate,''),TWOAmount,@Timekey
		,49999,'Y'
		from IBL_ENPA_STGDB_V2.dbo.ACCOUNT_ALL_SOURCE_SYSTEM A INNER JOIN IBL_ENPA_DB_V2.DBO.DIMSOURCEDB B 
		ON A.SourceSystem = B.SourceName 
		and b.EffectiveFromTimeKey <= @Timekey and B.EffectiveToTimeKey >= @Timekey
	    WHERE ISNULL(A.IsTWO,'')='Y'
				AND ISNULL(A.CLOSED_DATE,'')<>''
		
		UNION
		
		select B.SourceAlt_Key,cast(A.customerid as varchar(50)) + '_' + cast(B.SourceAlt_key as varchar(50)),
		A.CustomerAcID,'DCCO',ISNULL(TWODate,''),TWOAmount,@Timekey,49999,'Y'
		from IBL_ENPA_STGDB_V2.dbo.ACCOUNT_ALL_SOURCE_SYSTEM A INNER JOIN IBL_ENPA_DB_V2.DBO.DIMSOURCEDB B 
		ON A.SourceSystem = B.SourceName 
		and b.EffectiveFromTimeKey <= @Timekey and B.EffectiveToTimeKey >= @Timekey
	    WHERE ISNULL(A.DCCO_Date,'') IS NOT NULL
				AND ISNULL(A.CLOSED_DATE,'')<>''
		
		UNION

		select B.SourceAlt_Key,cast(A.customerid as varchar(50)) + '_' + cast(B.SourceAlt_key as varchar(50)),
		A.CustomerAcID,'PROJ COMP STATUS',ISNULL(TWODate,''),TWOAmount,@Timekey,49999,'Y'
		from IBL_ENPA_STGDB_V2.dbo.ACCOUNT_ALL_SOURCE_SYSTEM A INNER JOIN IBL_ENPA_DB_V2.DBO.DIMSOURCEDB B 
		ON A.SourceSystem = B.SourceName 
		and b.EffectiveFromTimeKey <= @Timekey and B.EffectiveToTimeKey >= @Timekey
	    WHERE ISNULL(A.PROJ_COMPLETION_DATE,'') IS NOT NULL
				AND ISNULL(A.CLOSED_DATE,'')<>''
				
		UNION
		
		select B.SourceAlt_Key,cast(A.customerid as varchar(50)) + '_' + cast(B.SourceAlt_key as varchar(50)),
		A.CustomerAcID,'OTS',ISNULL(TWODate,''),TWOAmount,@Timekey,49999,'Y'
		from IBL_ENPA_STGDB_V2.dbo.ACCOUNT_ALL_SOURCE_SYSTEM A INNER JOIN IBL_ENPA_DB_V2.DBO.DIMSOURCEDB B 
		ON A.SourceSystem = B.SourceName 
		and b.EffectiveFromTimeKey <= @Timekey and B.EffectiveToTimeKey >= @Timekey
	    WHERE ISNULL(A.IsOTS,'') ='Y'
				AND ISNULL(A.CLOSED_DATE,'')<>''
				
		UNION		
				
		select B.SourceAlt_Key,cast(A.customerid as varchar(50)) + '_' + cast(B.SourceAlt_key as varchar(50)),
		A.CustomerAcID,'Fraud Committed',ISNULL(TWODate,''),TWOAmount,@Timekey,49999,'Y'
		from IBL_ENPA_STGDB_V2.dbo.ACCOUNT_ALL_SOURCE_SYSTEM A INNER JOIN IBL_ENPA_DB_V2.DBO.DIMSOURCEDB B 
		ON A.SourceSystem = B.SourceName 
		and b.EffectiveFromTimeKey <= @Timekey and B.EffectiveToTimeKey >= @Timekey
	    WHERE ISNULL(A.FRAUDCOMMITTED,'')='Y'
				AND ISNULL(A.CLOSED_DATE,'')<>''

		UNION		
				
		select B.SourceAlt_Key,cast(A.customerid as varchar(50)) + '_' + cast(B.SourceAlt_key as varchar(50)),
		A.CustomerAcID,'SALE TO ARC',ISNULL(TWODate,''),TWOAmount,@Timekey,49999,'Y'
		from IBL_ENPA_STGDB_V2.dbo.ACCOUNT_ALL_SOURCE_SYSTEM A INNER JOIN IBL_ENPA_DB_V2.DBO.DIMSOURCEDB B 
		ON A.SourceSystem = B.SourceName 
		and b.EffectiveFromTimeKey <= @Timekey and B.EffectiveToTimeKey >= @Timekey
	    WHERE ISNULL(A.IsARC_Sale,'')='Y'
				AND ISNULL(A.CLOSED_DATE,'')<>''
	
		--UNION 
		--select B.SourceAlt_Key,cast(A.customerid as varchar(50)) + '_' + cast(B.SourceAlt_key as varchar(50)),A.CustomerAcID,'TWO',ISNULL(TWODate,''),TWOAmount,@Timekey,49999,'Y'
		--from IBL_ENPA_STGDB_V2.dbo.BRNET_ACCOUNT_STG A INNER JOIN IBL_ENPA_DB_V2.DBO.DIMSOURCEDB B 
		--ON A.SourceSystem = B.SourceName and b.EffectiveFromTimeKey <= @Timekey and B.EffectiveToTimeKey >= @Timekey
		--where SubAssetClassCode like '%W%'
		--AND ISNULL(A.CLOSED_DATE,'')<>'Y'


		 update A
			SET A.AccountEntityId=b.AccountEntityId
		 from IBL_ENPA_TEMPDB_V2.dbo.TempExceptionFinalStatusType a
			inner join IBL_ENPA_TEMPDB_V2.dbo.TempAdvAcBasicDetail b
				on a.ACID=b.CustomerACID
END


GO