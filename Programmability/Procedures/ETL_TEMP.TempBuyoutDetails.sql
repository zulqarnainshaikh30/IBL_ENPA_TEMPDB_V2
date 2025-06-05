SET QUOTED_IDENTIFIER, ANSI_NULLS ON
GO

/*************************************
Created by: Liyaqat
Created on: 2022-01-31
EXEC [ETL_Main].[BuyoutDetails_Final]
************************************/

CREATE PROC [ETL_TEMP].[TempBuyoutDetails] 
AS

BEGIN

SET NOCOUNT ON;

DECLARE  @vEffectivefrom  Int SET @vEffectiveFrom=(select Timekey from IBL_ENPA_DB_V2.[dbo].SYSDATAMATRIX WHERE CurrentStatus='C')
DECLARE @DATE AS DATE =(SELECT DATE FROM IBL_ENPA_DB_V2.[dbo].SYSDATAMATRIX WHERE TimeKey=@vEffectivefrom)

--select @DATE,@vEffectivefrom
--------------------------------------------------------------------------------------------------------------------------------------------------- 
  
  TRUNCATE TABLE IBL_ENPA_TEMPDB_V2.[dbo].[TempBuyoutDetails]

INSERT INTO IBL_ENPA_TEMPDB_V2.[dbo].[TempBuyoutDetails]
		(
			 BuyOutEntityID
			,ReferenceNo
			,PoolName
			,Category
			,BuyoutPartyLoanNo
			,CustomerName
			,PAN
			,AadharNo
			,PrincipalOutstanding
			,InterestReceivable
			,Charges
			,AccuredInterest
			,DPD
			,AssetClass
			,AuthorisationStatus	
			,EffectiveFromTimeKey	
			,EffectiveToTimeKey		
			,DateCreated
			,CreatedBy
			,MainCustomer
			,NPADate
			,SecurityValue
			,InterestOverdue
			,DailyInterestAccrualAmount
			,InterestSuspendedAmount
			,SuspendedInterestAmount
		)

		SELECT
			0 as BuyOutEntityID
			,ReferenceNo
			,PoolName
			,Category
			,BuyoutPartyLoanNo
			,CustomerName
			--,LEFT(PAN,10) PAN
			,case when PAN LIKE '%[A-Z][A-Z][A-Z][A-Z][A-Z][0-9][0-9][0-9][0-9][A-Z]%' Then LEFT(PAN,10)
						Else NULL end as PAN
			,LEFT(replace (AadharNo,' ',''),12) AadharNo 
			,Case When PrincipalOutstanding <>'' THEN cast(REPLACE(PrincipalOutstanding,',','') as DECIMAL(30,2)) ELSE NULL END  PrincipalOutstanding
			,Case When InterestReceivable <>'' THEN cast(InterestReceivable as DECIMAL(30,2)) ELSE NULL END  InterestReceivable
			,Case When Charges <>'' THEN CAST(Charges AS DECIMAL(30,2)) ELSE NULL END   Charges
			,Case When AccuredInterest <>'' THEN CAST(AccuredInterest AS DECIMAL(30,2)) ELSE NULL END  AccuredInterest
			,DPD
			,AssetClass
			,'NP'	
			,@vEffectivefrom
			,49999	
			,GETDATE()
			,'D2K'
			,MainCustomer
			,Case When NPADate <>''THEN NPADate ELSE NULL END  as NPADate
			,Case When SecurityValue <>'' THEN CAST(ISNULL(CAST(SecurityValue AS DECIMAL(30,2)),0) AS DECIMAL(30,2)) ELSE NULL END SecurityValue
			,Case When InterestOverdue<>'' THEN CAST(ISNULL(CAST(InterestOverdue AS DECIMAL(30,2)),0) AS DECIMAL(30,2)) ELSE NULL END InterestOverdue
			----------------==Below columns==added by Pranay=======  2023-03-17
			,0 DailyInterestAccrualAmount
			,0 InterestSuspendedAmount
			,0 SuspendedInterestAmount

	FROM IBL_ENPA_STGDB_V2.[dbo].[BuyoutDetails_stg]
	
		--==============Delete Duplicate A/Cs==================added by LQT=======  2023-07-05
		;with cte_dup
			as
			(select BuyoutPartyLoanNo, ROW_NUMBER() over( PARTITION by BuyoutPartyLoanNo order by BuyoutPartyLoanNo)  RwNo from IBL_ENPA_TEMPDB_V2.[dbo].[TempBuyoutDetails] )
			Delete from cte_dup where RwNo>1

		--==============Update BuyOutEntityID for existing A/Cs==================added by Pranay=======  2023-03-17

Update TABD
   Set BuyOutEntityID=ABD.BuyOutEntityID
from IBL_ENPA_TEMPDB_V2.[dbo].[TempBuyoutDetails] TABD
Inner Join IBL_ENPA_DB_V2.dbo.[BuyoutDetails_Final] ABD 
On TABD.BuyoutPartyLoanNo=ABD.BuyoutPartyLoanNo AND ABD.EffectiveToTimeKey=49999



DECLARE @MAX INT
SET @MAX = (select MAX(BuyOutEntityID) BuyOutEntityID from(
(SELECT ISNULL(MAX(BuyOutEntityID),0)BuyOutEntityID FROM IBL_ENPA_DB_V2.Curdat.BuyoutDetails_Final)
Union All
(SELECT ISNULL(MAX(BuyOutEntityID),0)BuyOutEntityID FROM IBL_ENPA_TEMPDB_V2.dbo.[TempBuyoutDetails])
--Union All
--(SELECT ISNULL(MAX(AccountEntityId),0)AccountEntityId FROM IBL_ENPA_DB_V2.Curdat.AdvNFAcBasicDetail)
)A
)
--Select @Max
UPDATE IBL_ENPA_TEMPDB_V2.Dbo.[TempBuyoutDetails]
SET @MAX=BuyOutEntityID=@MAX+1
WHERE BuyOutEntityID =0--IS NULL


End
GO