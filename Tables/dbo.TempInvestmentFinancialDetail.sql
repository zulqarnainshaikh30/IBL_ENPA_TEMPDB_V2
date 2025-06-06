﻿CREATE TABLE [dbo].[TempInvestmentFinancialDetail] (
  [Time_Key] [int] NULL,
  [Inv_Key] [tinyint] NULL,
  [HoldingNature] [char](3) NULL,
  [CurrencyAlt_Key] [tinyint] NULL,
  [CurrencyConvRate] [smallint] NULL,
  [BookValue] [decimal](18, 2) NULL,
  [BookValueINR] [decimal](18, 2) NULL,
  [MTMValue] [decimal](18, 2) NULL,
  [MTMValueINR] [decimal](18, 2) NULL,
  [EncumberedMTM] [decimal](18, 2) NULL,
  [AssetClass_AltKey] [tinyint] NULL,
  [NPIDt] [date] NULL,
  [TotalProvison] [decimal](18, 2) NULL,
  [AuthorisationStatus] [varchar](2) NULL,
  [EffectiveFromTimeKey] [int] NULL,
  [EffectiveToTimeKey] [int] NULL,
  [CreatedBy] [varchar](100) NULL,
  [DateCreated] [smalldatetime] NULL,
  [ModifiedBy] [varchar](100) NULL,
  [DateModified] [smalldatetime] NULL,
  [ApprovedBy] [varchar](100) NULL,
  [DateApproved] [smalldatetime] NULL,
  [Ischanged] [char](1) NULL,
  [InvEntityId] [int] NULL,
  [EntityKey] [int] IDENTITY,
  [RefInvID] [varchar](100) NULL,
  [DBTDate] [date] NULL,
  [LatestBSDate] [date] NULL,
  [Interest_DividendDueDate] [date] NULL,
  [Interest_DividendDueAmount] [decimal](16, 2) NULL,
  [PartialRedumptionDueDate] [date] NULL,
  [PartialRedumptionSettledY_N] [char](1) NULL,
  [FLGDEG] [char](1) NULL,
  [DEGREASON] [varchar](500) NULL,
  [DPD] [int] NULL,
  [FLGUPG] [char](1) NULL,
  [UpgDate] [date] NULL,
  [PROVISIONALT_KEY] [int] NULL,
  [InitialAssetAlt_Key] [int] NULL,
  [InitialNPIDt] [date] NULL,
  [RefIssuerID] [varchar](100) NULL,
  [DPD_Maturity] [smallint] NULL,
  [DPD_DivOverdue] [int] NULL,
  [FinalAssetClassAlt_Key] [int] NULL,
  [PartialRedumptionDPD] [smallint] NULL,
  [ISIN] [varchar](30) NULL,
  [FlgSMA] [char](1) NULL,
  [SMA_Dt] [date] NULL,
  [SMA_Class] [varchar](5) NULL,
  [SMA_Reason] [varchar](1000) NULL
)
ON [PRIMARY]
GO