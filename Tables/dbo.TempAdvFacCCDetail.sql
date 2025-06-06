﻿CREATE TABLE [dbo].[TempAdvFacCCDetail] (
  [ENTITYKEY] [bigint] NULL,
  [AccountEntityId] [int] NOT NULL,
  [AdhocDt] [date] NULL,
  [AdhocAmt] [decimal](14) NULL,
  [ContExcsSinceDt] [date] NULL,
  [DerecognisedInterest1] [decimal](14) NULL,
  [DerecognisedInterest2] [decimal](14) NULL,
  [ClaimType] [varchar](8) NULL,
  [ClaimCoverAmt] [decimal](14) NULL,
  [ClaimLodgedDt] [date] NULL,
  [ClaimLodgedAmt] [decimal](14) NULL,
  [ClaimRecvDt] [date] NULL,
  [ClaimReceivedAmt] [decimal](14) NULL,
  [ClaimRate] [decimal](14) NULL,
  [RefSystemAcid] [varchar](30) NULL,
  [AuthorisationStatus] [varchar](2) NULL,
  [EffectiveFromTimeKey] [int] NOT NULL,
  [EffectiveToTimeKey] [int] NOT NULL,
  [CreatedBy] [varchar](20) NULL,
  [DateCreated] [smalldatetime] NULL,
  [ModifiedBy] [varchar](20) NULL,
  [DateModified] [smalldatetime] NULL,
  [ApprovedBy] [varchar](20) NULL,
  [DateApproved] [smalldatetime] NULL,
  [D2Ktimestamp] [timestamp],
  [MocStatus] [char](1) NULL,
  [MocDate] [smalldatetime] NULL,
  [MocTypeAlt_Key] [int] NULL,
  [AdhocExpiryDate] [date] NULL,
  [Ischanged] [char](1) NULL,
  [StockStmtDt] [date] NULL
)
ON [PRIMARY]
GO