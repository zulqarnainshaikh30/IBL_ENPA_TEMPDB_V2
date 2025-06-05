CREATE TABLE [dbo].[TempAdvSecurityValueDetail] (
  [ENTITYKEY] [bigint] NULL,
  [SecurityEntityID] [bigint] NULL,
  [ValuationSourceAlt_Key] [smallint] NULL,
  [ValuationDate] [datetime] NULL,
  [CurrentValue] [decimal](16, 2) NULL,
  [ValuationExpiryDate] [datetime] NULL,
  [AuthorisationStatus] [char](2) NULL,
  [EffectiveFromTimeKey] [int] NOT NULL,
  [EffectiveToTimeKey] [int] NOT NULL,
  [CreatedBy] [varchar](20) NULL,
  [DateCreated] [smalldatetime] NULL,
  [ModifiedBy] [varchar](20) NULL,
  [DateModified] [smalldatetime] NULL,
  [ApprovedBy] [varchar](20) NULL,
  [DateApproved] [smalldatetime] NULL,
  [D2Ktimestamp] [datetime] NULL,
  [Ischanged] [char](1) NULL
)
ON [PRIMARY]
GO