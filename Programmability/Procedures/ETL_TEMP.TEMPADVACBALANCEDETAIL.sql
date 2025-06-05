SET QUOTED_IDENTIFIER, ANSI_NULLS ON
GO
-- =============================================  
-- Author:  Sudip Chakraborty  
-- Create date: 19-02-2021  
-- Description: Insert AdvAcBalanceDetail  
-- =============================================  
  
  
  
CREATE PROCEDURE  [ETL_TEMP].[TEMPADVACBALANCEDETAIL]  
   
AS  
BEGIN  
 -- SET NOCOUNT ON added to prevent extra result sets from  
   
 SET NOCOUNT ON;  
  
  
   
  
  
TRUNCATE TABLE [IBL_ENPA_TEMPDB_V2].[DBO].[TEMPADVACBALANCEDETAIL]  
  
DECLARE  @vEffectivefrom  Int SET @vEffectiveFrom=(SELECT TimeKey FROM IBL_ENPA_DB_V2.[dbo].Automate_Advances WHERE EXT_FLG='Y')  
DECLARE @TimeKey  Int SET @TimeKey=(SELECT TimeKey FROM IBL_ENPA_DB_V2.[dbo].Automate_Advances WHERE EXT_FLG='Y')  
DECLARE @DATE AS DATE =(SELECT Date FROM IBL_ENPA_DB_V2.[dbo].Automate_Advances WHERE EXT_FLG='Y')  
  
----------------------------------------------------------------------------------------------------------------------------------------------------  
/* For All Accounts */   
 
SET IDENTITY_INSERT [IBL_ENPA_TEMPDB_V2].[DBO].[TEMPADVACBALANCEDETAIL] ON

INSERT INTO [IBL_ENPA_TEMPDB_V2].[DBO].[TEMPADVACBALANCEDETAIL]  
(  
  
   ACCOUNTENTITYID  
  ,ASSETCLASSALT_KEY  
  ,BALANCEINCURRENCY  
  ,BALANCE  
  ,SIGNBALANCE  
  ,LASTCRDT  
  ,OVERDUE  
  ,TOTALPROV  
  ,REFCUSTOMERID  
  ,REFSYSTEMACID  
  ,AUTHORISATIONSTATUS  
  ,EFFECTIVEFROMTIMEKEY  
  ,EFFECTIVETOTIMEKEY  
  ,OVERDUESINCEDT  
  ,MOCSTATUS  
  ,MOCDATE  
  ,MOCTYPEALT_KEY  
  ,OLD_OVERDUESINCEDT  
  ,OLD_OVERDUE  
  ,ORG_TOTALPROV  
  ,INTREVERSEAMT  
  ,PS_BALANCE  
  ,NPS_BALANCE  
  ,DATECREATED  
  ,MODIFIEDBY  
  ,DATEMODIFIED  
  ,APPROVEDBY  
  ,DATEAPPROVED  
  ,CREATEDBY  
  ,PS_NPS_FLAG  
  ,UPGRADEDATE  
  ,OverduePrincipal  
  ,Ischanged  
  ,Overdueinterest  
  ,DFVAmt  
  ,InterestReceivable  
  ,OverduePrincipalDt  
  ,OverdueIntDt  
  ,OverOtherdue  
  ,OverdueOtherDt  
  ,SourceAssetClass  
  ,PrincipalBalance  
  ,SourceNpaDate  
  )  
  
  -----------------------CBS TL DATA-----------------------  
  SELECT   
  
  ACBD.ACCOUNTENTITYID AS  ACCOUNTENTITYID  
  ,DA.AssetClassAlt_Key AS ASSETCLASSALT_KEY  
  ,ACC1.BalanceInActualAcCurrency AS BALANCEINCURRENCY  
  ,ACC1.BalanceOutstandingINR AS BALANCE  
  ,ACC1.BalanceOutstandingINR AS SIGNBALANCE  
  ,ACC1.LastCreditDate AS LASTCRDT  
  ,NULL AS OVERDUE   
  ,NULL AS TOTALPROV  
  ,ACBD.REFCUSTOMERID [REFCUSTOMERID]  
  ,ACBD.SYSTEMACID [REFSYSTEMACID]  
  ,ACBD.AuthorisationStatus  
  ,@VEFFECTIVEFROM [EFFECTIVEFROMTIMEKEY]  
  ,49999 [EFFECTIVETOTIMEKEY]  
  ,NULL AS OVERDUESINCEDT    
  ,'N' AS MOCSTATUS  
  ,NULL AS MOCDATE  
  ,NULL AS MOCTYPEALT_KEY  
  ,NULL AS OLD_OVERDUESINCEDT  
  ,NULL AS OLD_OVERDUE  
  ,NULL AS ORG_TOTALPROV  
  ,NULL AS INTREVERSEAMT  
  ,0 AS PS_BALANCE  
  ,0 AS NPS_BALANCE  
  ,GETDATE() AS DATECREATED  
  ,NULL AS MODIFIEDBY  
  ,NULL AS DATEMODIFIED  
  ,NULL AS APPROVEDBY  
  ,NULL AS DATEAPPROVED  
  ,'SSISUSER' AS CREATEDBY  
  ,NULL AS PS_NPS_FLAG  
  ,NULL AS UPGRADEDATE  
  ,ISNULL(ACC1.PrincipalOverdueAmt,0)  AS OverduePrincipal    
  ,NULL AS Ischanged   
  ,ACC1.InterestOverdueAmt Overdueinterest  
  ,ACC1.DFVAmt DFVAmt  
  ,NULL InterestReceivable  
  ,ACC1.PrincipalOverDueSinceDt OverduePrincipalDt  
  ,ACC1.InterestOverDueSinceDt OverdueIntDt  
  ,ACC1.OthChargesOverdueAmt OverOtherdue  
  ,ACC1.OthChangesOverDueSinceDt OverdueOtherDt  
  ,ACC1.AssetClassCode SourceAssetClass  
  ,ACC1.POSBalance  
  ,ACC1.NPADate SourceNpaDate  
  FROM IBL_ENPA_STGDB_V2.DBO.ACCOUNT_ALL_SOURCE_SYSTEM ACC1  
  INNER JOIN IBL_ENPA_TEMPDB_V2.DBO.TempAdvAcBasicDetail ACBD ON  ACC1.CustomerAcID=ACBD.CustomerACID  
  LEFT JOIN IBL_ENPA_DB_V2.DBO.DimAssetClassMapping DA ON ACC1.AssetClassCode=DA.SrcSysClassCode  
  AND DA.EffectiveFromTimeKey<=@TimeKey AND DA.EffectiveToTimeKey>=@TimeKey  

SET IDENTITY_INSERT [IBL_ENPA_TEMPDB_V2].[DBO].[TEMPADVACBALANCEDETAIL] OFF
/*UNION  
----------------------CBS CC DATA-----------------------  
SELECT   
  
  ACBD.ACCOUNTENTITYID AS  ACCOUNTENTITYID  
  ,DA.AssetClassAlt_Key AS ASSETCLASSALT_KEY  
  ,ACC2.BalanceInActualAcCurrency AS BALANCEINCURRENCY  
  ,ACC2.BalanceOutstandingINR AS BALANCE  
  ,ACC2.BalanceOutstandingINR AS SIGNBALANCE  
  ,ACC2.LastCreditDate AS LASTCRDT  
  ,NULL AS OVERDUE   
  ,NULL AS TOTALPROV  
  ,ACBD.REFCUSTOMERID [REFCUSTOMERID]  
  ,ACBD.SYSTEMACID [REFSYSTEMACID]  
  ,ACBD.AuthorisationStatus  
  ,@VEFFECTIVEFROM [EFFECTIVEFROMTIMEKEY]  
  ,49999 [EFFECTIVETOTIMEKEY]  
  ,NULL AS OVERDUESINCEDT  
  ,'N' AS MOCSTATUS  
  ,NULL AS MOCDATE  
  ,NULL AS MOCTYPEALT_KEY  
  ,NULL AS OLD_OVERDUESINCEDT  
  ,NULL AS OLD_OVERDUE  
  ,NULL AS ORG_TOTALPROV  
  ,NULL AS INTREVERSEAMT  
  ,0 AS PS_BALANCE  
  ,0 AS NPS_BALANCE  
  ,GETDATE() AS DATECREATED  
  ,NULL AS MODIFIEDBY  
  ,NULL AS DATEMODIFIED  
  ,NULL AS APPROVEDBY  
  ,NULL AS DATEAPPROVED  
  ,'SSISUSER' AS CREATEDBY  
  ,NULL AS PS_NPS_FLAG  
  ,NULL AS UPGRADEDATE  
  ,ISNULL(ACC2.PrincipalOverdueAmt,0)  AS OverduePrincipal  
  ,NULL AS Ischanged   
  ,ACC2.InterestOverdueAmt Overdueinterest  
  ,ACC2.DFVAmt DFVAmt  
  ,NULL InterestReceivable  
  ,ACC2.PrincipalOverDueSinceDt OverduePrincipalDt  
  ,ACC2.InterestOverDueSinceDt OverdueIntDt  
  ,ACC2.OthChargesOverdueAmt OverOtherdue  
  ,ACC2.OthChangesOverDueSinceDt OverdueOtherDt  
  ,ACC2.AssetClassCode SourceAssetClass  
  ,(ISNULL(ACC2.BalanceOutstandingINR,0)-ISNULL(ACC2.InterestOverdueAmt,0)-ISNULL(ACC2.OthChargesOverdueAmt,0)) POSBalance  
  ,ACC2.NPADate SourceNpaDate  
  FROM IBL_ENPA_STGDB_V2.DBO.ACCOUNT_SOURCESYSTEM02_STG ACC2  
  INNER JOIN IBL_ENPA_TEMPDB_V2.DBO.TempAdvAcBasicDetail ACBD ON  ACC2.CustomerAcID=ACBD.CustomerACID  
  LEFT JOIN IBL_ENPA_DB_V2.DBO.DimAssetClassMapping DA ON ACC2.AssetClassCode=DA.SrcSysClassCode  
  AND DA.EffectiveFromTimeKey<=@TimeKey AND DA.EffectiveToTimeKey>=@TimeKey  
  
UNION  
---------------------- CREDIT CARD DATA-----------------------  
SELECT   
  
  ACBD.ACCOUNTENTITYID AS  ACCOUNTENTITYID  
  --,DA.AssetClassAlt_Key AS ASSETCLASSALT_KEY  
  ,Case when ACC6.AssetClassCode=0 then 1   
    when ACC6.AssetClassCode in (1,2) then 2 end As ASSETCLASSALT_KEY  
  ,ACC6.BalanceInActualAcCurrency AS BALANCEINCURRENCY  
  ,ACC6.BalanceOutstandingINR AS BALANCE  
  ,ACC6.BalanceOutstandingINR AS SIGNBALANCE  
  ,NULL AS LASTCRDT  
  ,NULL AS OVERDUE  
  ,NULL AS TOTALPROV  
  ,ACBD.REFCUSTOMERID [REFCUSTOMERID]  
  ,ACBD.SYSTEMACID [REFSYSTEMACID]  
  ,ACBD.AuthorisationStatus  
  ,@VEFFECTIVEFROM [EFFECTIVEFROMTIMEKEY]  
  ,49999 [EFFECTIVETOTIMEKEY]  
  ,NULL AS OVERDUESINCEDT   
  ,'N' AS MOCSTATUS  
  ,NULL AS MOCDATE  
  ,NULL AS MOCTYPEALT_KEY  
  ,NULL AS OLD_OVERDUESINCEDT  
  ,NULL AS OLD_OVERDUE  
  ,NULL AS ORG_TOTALPROV  
  ,NULL AS INTREVERSEAMT  
  ,0 AS PS_BALANCE  
  ,0 AS NPS_BALANCE  
  ,GETDATE() AS DATECREATED  
  ,NULL AS MODIFIEDBY  
  ,NULL AS DATEMODIFIED  
  ,NULL AS APPROVEDBY  
  ,NULL AS DATEAPPROVED  
  ,'SSISUSER' AS CREATEDBY  
  ,NULL AS PS_NPS_FLAG  
  ,NULL AS UPGRADEDATE  
  ,ISNULL(ACC6.PrincipalOverdueAmt,0)  AS OverduePrincipal   
  ,NULL AS Ischanged  
  ,ACC6.InterestOverdueAmt Overdueinterest  
  ,NULL DFVAmt  
  ,NULL InterestReceivable  
  ,ACC6.PrincipalOverDueSinceDt OverduePrincipalDt  
  ,ACC6.InterestOverDueSinceDt OverdueIntDt  
  ,ACC6.OthChargesOverdueAmt OverOtherdue  
  ,ACC6.OthChangesOverDueSinceDt OverdueOtherDt  
  ,ACC6.AssetClassCode SourceAssetClass  
  ,ACC6.Balance_POS  
  ,ACC6.NPADate SourceNpaDate  
  FROM IBL_ENPA_STGDB_V2.DBO.ACCOUNT_SOURCESYSTEM06_STG ACC6  
  INNER JOIN IBL_ENPA_TEMPDB_V2.DBO.TempAdvAcBasicDetail ACBD ON  ACC6.CustomerAcID=ACBD.CustomerACID  
  LEFT JOIN IBL_ENPA_DB_V2.DBO.DimAssetClassMapping DA ON ACC6.AssetClassCode=DA.SrcSysClassCode  
  AND DA.EffectiveFromTimeKey<=@TimeKey AND DA.EffectiveToTimeKey>=@TimeKey  
  
 UNION   
--------------Added on 15-04-2021 for Bills  
SELECT   
  
  ACBD.ACCOUNTENTITYID AS  ACCOUNTENTITYID  
  ,DA.AssetClassAlt_Key AS ASSETCLASSALT_KEY  
  ,ACC1.BalanceInActualAcCurrency AS BALANCEINCURRENCY  
  ,ACC1.BalanceOutstandingINR AS BALANCE  
  ,ACC1.BalanceOutstandingINR AS SIGNBALANCE  
  ,ACC1.LastCreditDate AS LASTCRDT  
  ,NULL AS OVERDUE   
  ,NULL AS TOTALPROV  
  ,ACBD.REFCUSTOMERID [REFCUSTOMERID]  
  ,ACBD.SYSTEMACID [REFSYSTEMACID]  
  ,ACBD.AuthorisationStatus  
  ,@VEFFECTIVEFROM [EFFECTIVEFROMTIMEKEY]  
  ,49999 [EFFECTIVETOTIMEKEY]  
  ,NULL AS OVERDUESINCEDT    
  ,'N' AS MOCSTATUS  
  ,NULL AS MOCDATE  
  ,NULL AS MOCTYPEALT_KEY  
  ,NULL AS OLD_OVERDUESINCEDT  
  ,NULL AS OLD_OVERDUE  
  ,NULL AS ORG_TOTALPROV  
  ,NULL AS INTREVERSEAMT  
  ,0 AS PS_BALANCE  
  ,0 AS NPS_BALANCE  
  ,GETDATE() AS DATECREATED  
  ,NULL AS MODIFIEDBY  
  ,NULL AS DATEMODIFIED  
  ,NULL AS APPROVEDBY  
  ,NULL AS DATEAPPROVED  
  ,'SSISUSER' AS CREATEDBY  
  ,NULL AS PS_NPS_FLAG  
  ,NULL AS UPGRADEDATE  
  ,ISNULL(ACC1.PrincipalOverdueAmt,0)  AS OverduePrincipal    
  ,NULL AS Ischanged  
  ,ACC1.InterestOverdueAmt Overdueinterest  
  ,ACC1.DFVAmt DFVAmt  
  ,NULL InterestReceivable  
  ,ACC1.PrincipalOverDueSinceDt OverduePrincipalDt  
  ,ACC1.InterestOverDueSinceDt OverdueIntDt  
  ,ACC1.OthChargesOverdueAmt OverOtherdue  
  ,ACC1.OthChangesOverDueSinceDt OverdueOtherDt  
  ,ACC1.AssetClassCode SourceAssetClass  
  ,ACC1.POSBalance  
  ,ACC1.NPADate SourceNpaDate  
  FROM IBL_ENPA_STGDB_V2.DBO.ACCOUNT_SOURCESYSTEM07_STG ACC1  
  INNER JOIN IBL_ENPA_TEMPDB_V2.DBO.TempAdvAcBasicDetail ACBD ON  ACC1.CustomerAcID=ACBD.CustomerACID  
  LEFT JOIN IBL_ENPA_DB_V2.DBO.DimAssetClassMapping DA ON ACC1.AssetClassCode=DA.SrcSysClassCode  
  AND DA.EffectiveFromTimeKey<=@TimeKey AND DA.EffectiveToTimeKey>=@TimeKey  
  */
---------Added on 20-04-2021 Update Balance = 0 where Balance<0  
  
Update [IBL_ENPA_TEMPDB_V2].[DBO].[TEMPADVACBALANCEDETAIL] set Balance=0,BalanceInCurrency=0 where Balance<0  
  
  
---------------------------Added On 23-04-2021 -----OverDueSinceDt Update  
  
  
Update D set OverDueSinceDt=OverduePrincipalDt  
    ,OverDue=(ISNULL(OverduePrincipal,0)+ISNULL(Overdueinterest,0))  
from IBL_ENPA_TEMPDB_V2.dbo.TempAdVAcBalanceDetail D  
Inner Join IBL_ENPA_TEMPDB_V2.dbo.TempAdvAcBasicDetail A ON D.AccountEntityId=A.AccountEntityId  
Inner Join IBL_ENPA_DB_V2.dbo.DimProduct C ON A.ProductAlt_Key=C.ProductAlt_Key And C.EffectiveToTimeKey=49999  
Inner Join IBL_ENPA_DB_V2.dbo.DimGLProduct_AU B ON C.ProductCode=B.ProductCode And B.EffectiveToTimeKey=49999  
where OverduePrincipalDt is not null And OverdueIntDt Is null  
/*And B.ProductCode not in (  
'12211','20101','20210','11008','12209','20216','20217','20116','20206','12308','12210','20203','20135'  
,'12205','20115','20205','20102','12302','20103','12315','20215','12208','12310','12312','20214','20231'  
,'20117','20121','20201','20202','20108','11006','20209','20238','12201','20105','12207','20110','12311'  
,'12225','12309','12314','12204','20123','20124','12318','20211','20122','20220','20226','20207','20134'  
,'20219','20241','20109','20120','20223','20118','20112','20119','12218','12202','20222','20228','12313'  
,'11303','12508','20104','20132','20244','12317','20227','12301','20212','20213','20204','11305','12219'  
,'20224','20221','20225','20229')   
 */

Update D set OverDueSinceDt=OverdueIntDt  
    ,OverDue=(ISNULL(OverduePrincipal,0)+ISNULL(Overdueinterest,0))  
from IBL_ENPA_TEMPDB_V2.dbo.TempAdVAcBalanceDetail  D  
Inner Join IBL_ENPA_TEMPDB_V2.dbo.TempAdvAcBasicDetail A ON D.AccountEntityId=A.AccountEntityId  
Inner Join IBL_ENPA_DB_V2.dbo.DimProduct C ON A.ProductAlt_Key=C.ProductAlt_Key And C.EffectiveToTimeKey=49999  
Inner Join IBL_ENPA_DB_V2.dbo.DimGLProduct_AU B ON C.ProductCode=B.ProductCode And B.EffectiveToTimeKey=49999  
where OverduePrincipalDt is null And OverdueIntDt Is not null  
/*And B.ProductCode not in (  
'12211','20101','20210','11008','12209','20216','20217','20116','20206','12308','12210','20203','20135'  
,'12205','20115','20205','20102','12302','20103','12315','20215','12208','12310','12312','20214','20231'  
,'20117','20121','20201','20202','20108','11006','20209','20238','12201','20105','12207','20110','12311'  
,'12225','12309','12314','12204','20123','20124','12318','20211','20122','20220','20226','20207','20134'  
,'20219','20241','20109','20120','20223','20118','20112','20119','12218','12202','20222','20228','12313'  
,'11303','12508','20104','20132','20244','12317','20227','12301','20212','20213','20204','11305','12219'  
,'20224','20221','20225','20229')   
  */
Update IBL_ENPA_TEMPDB_V2.dbo.TempAdVAcBalanceDetail set OverDueSinceDt=(Case when OverduePrincipalDt<=OverdueIntDt then OverduePrincipalDt Else OverdueIntDt End)  
    ,OverDue=(ISNULL(OverduePrincipal,0)+ISNULL(Overdueinterest,0))  
from IBL_ENPA_TEMPDB_V2.dbo.TempAdVAcBalanceDetail  D  
Inner Join IBL_ENPA_TEMPDB_V2.dbo.TempAdvAcBasicDetail A ON D.AccountEntityId=A.AccountEntityId  
Inner Join IBL_ENPA_DB_V2.dbo.DimProduct C ON A.ProductAlt_Key=C.ProductAlt_Key And C.EffectiveToTimeKey=49999  
Inner Join IBL_ENPA_DB_V2.dbo.DimGLProduct_AU B ON C.ProductCode=B.ProductCode And B.EffectiveToTimeKey=49999   
where OverduePrincipalDt is not null And OverdueIntDt Is not null  
/*And B.ProductCode not in (  
'12211','20101','20210','11008','12209','20216','20217','20116','20206','12308','12210','20203','20135'  
,'12205','20115','20205','20102','12302','20103','12315','20215','12208','12310','12312','20214','20231'  
,'20117','20121','20201','20202','20108','11006','20209','20238','12201','20105','12207','20110','12311'  
,'12225','12309','12314','12204','20123','20124','12318','20211','20122','20220','20226','20207','20134'  
,'20219','20241','20109','20120','20223','20118','20112','20119','12218','12202','20222','20228','12313'  
,'11303','12508','20104','20132','20244','12317','20227','12301','20212','20213','20204','11305','12219'  
,'20224','20221','20225','20229')   
  */
  
----Credit Card  
UPDATE A SET A.OVERDUESINCEDT=DATEADD(DAY,-Cast(DPD as Int)+1,@DATE)   -- 14/07/2022 Overduesince date issue added + 1
FROM IBL_ENPA_TEMPDB_V2.dbo.TempAdVAcBalanceDetail A INNER  JOIN   
IBL_ENPA_TEMPDB_V2.dbo.TempAdvAcBasicDetail C ON C.AccountEntityId=A.AccountEntityId  
INNER JOIN IBL_ENPA_STGDB_V2.dbo.ACCOUNT_ALL_SOURCE_SYSTEM B ON C.CustomerACID=B.CustomerAcID  
WHERE  Cast(ISNULL(B.DPD,0) as Int)>0  
  
  
-----------------------------------------  

--- CASA Accounts, overdue date balnk for non Agri cases _ 06 July 2022

UPDATE A SET A.OVERDUESINCEDT = NULL 
FROM IBL_ENPA_TEMPDB_V2.dbo.TempAdVAcBalanceDetail A INNER  JOIN   
IBL_ENPA_TEMPDB_V2.dbo.TempAdvAcBasicDetail C ON C.AccountEntityId=A.AccountEntityId  
INNER JOIN IBL_ENPA_STGDB_V2.dbo.ACCOUNT_ALL_SOURCE_SYSTEM B ON C.CustomerACID=B.CustomerAcID  
INNER JOIN IBL_ENPA_DB_V2.dbo.Dimproduct p on c.Productalt_key = p.Productalt_key and p.Effectivetotimekey = 49999
WHERE isnull(P.ProductSubGroup,'N') Not  in('Agri Busi','Agri TL','KCC')
AND C.FacilityType in ('CC','OD')

  
------Added on 25-02-2022 for bills on UnAppliedIntAmt  
/*  
Update A Set A.UnAppliedIntAmount=B.UnAppliedIntt  
 from IBL_ENPA_TEMPDB_V2.dbo.TempAdVAcBalanceDetail A   
INNER  JOIN IBL_ENPA_STGDB_V2.dbo.BILL_SOURCESYSTEM_STG B ON A.RefSystemAcId=B.AccountID  
  */
--------------------------  
  
  
  
-------------Added on 26-03-2021 FRom Bills  
  
  
--INSERT INTO [IBL_ENPA_TEMPDB_V2].[DBO].[TEMPADVACBALANCEDETAIL]  
--(AccountEntityId,RefSystemAcId,RefCustomerId,Balance,EffectiveFromTimeKey,EffectiveToTimeKey,CreatedBy,DateCreated)  
  
--Select   
--ACBD.AccountEntityId,B.AccountID,B.CustomerId,B.BalanceInINR,@VEFFECTIVEFROM,49999,'SSISUSER',GETDATE()  
  
--from IBL_ENPA_TEMPDB_V2.DBO.TempAdvAcBasicDetail ACBD INNER JOIN (  
--Select AccountID,CustomerId,SUM(ISNULL(BalanceInINR,0))BalanceInINR from IBL_ENPA_STGDB_V2.dbo.BILL_SOURCESYSTEM_STG where AccountID is not NULL  
--Group By AccountID,CustomerId  
--)B ON B.AccountID=ACBD.CustomerACID  
  
  
END  
GO