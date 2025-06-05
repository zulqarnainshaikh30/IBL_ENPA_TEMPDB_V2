SET QUOTED_IDENTIFIER, ANSI_NULLS ON
GO

CREATE PROCEDURE [ETL_TEMP].[INSERT_WRITEOFF_ALL_SOURCE_SYSTEM_TEMP]
AS
BEGIN

/*SELECT * INTO IBL_ENPA_TEMPDB_V2.ETL_TEMP.AdvAcWODetail FROM IBL_ENPA_DB_V2.CURDAT.AdvAcWODetail WHERE 1=2*/

TRUNCATE TABLE IBL_ENPA_TEMPDB_V2.ETL_TEMP.TempAdvAcWODetail

	/*MAINTAIN WRITE OFF DATA IN AdvAcWODetail TABLE*/
	DECLARE @TimeKey INT = (SELECT TimeKey FROM IBL_ENPA_DB_V2..SysDataMatrix WHERE CurrentStatus='C' )
	DECLARE @Exec_Date DATE=(SELECT DATE FROM IBL_ENPA_DB_V2..SysDataMatrix WHERE TimeKey=@TimeKey )

	
		INSERT INTO IBL_ENPA_TEMPDB_V2.ETL_TEMP.AdvAcWODetail (
				[EffectiveFromTimeKey],
				[EffectiveToTimeKey],
				Customer_CIF,
				[CustomerID],
				[CustomerACID],
				[WriteOffDt],
				[WO_PWO],
				[WriteOffAmt],
				CreatedBy,
				DateCreated)
		SELECT @TimeKey,
			   99999,
			   NCIF_ID,
			   A.CustomerID,
			   A.CustomerACID,
			   A.TWODate,
			   'TWO',
			   A.TWOAmount,
			   'SSIS USER',
			   GETDATE()
		FROM IBL_ENPA_STGDB_V2.DBO.ACCOUNT_ALL_SOURCE_SYSTEM A
		LEFT JOIN IBL_ENPA_DB_V2.[CURDAT].[AdvAcWODetail] WO  ON WO.EffectiveFromTimeKey<=@TimeKey
											 AND WO.EffectiveToTimeKey>=@TimeKey
											 AND Wo.CustomerID=A.CustomerId
											 AND WO.CustomerACID=A.CustomerACID
		WHERE ISNULL(A.IsTWO,'')='Y'
		AND WO.CustomerACID IS NULL
END
GO