CREATE TABLE [dbo].[TempAdvCreditCardBalanceDetail] (
  [EntityKey] [bigint] NULL,
  [AccountEntityId] [int] NULL,
  [CreditCardEntityId] [int] NULL,
  [Balance_POS] [decimal](16, 2) NULL,
  [Balance_LOAN] [decimal](16, 2) NULL,
  [Balance_INT] [decimal](16, 2) NULL,
  [Balance_GST] [decimal](16, 2) NULL,
  [Balance_FEES] [decimal](16, 2) NULL,
  [RefSystemAcId] [varchar](30) NULL,
  [AuthorisationStatus] [int] NULL,
  [EffectiveFromTimeKey] [int] NOT NULL,
  [EffectiveToTimeKey] [int] NOT NULL,
  [CreatedBy] [varchar](20) NULL,
  [DateCreated] [datetime] NOT NULL,
  [ModifiedBy] [int] NULL,
  [DateModified] [int] NULL,
  [ApprovedBy] [int] NULL,
  [DateApproved] [int] NULL,
  [D2Ktimestamp] [int] NULL,
  [MocStatus] [int] NULL,
  [MocDate] [int] NULL,
  [ISChanged] [varchar](1) NULL
)
ON [PRIMARY]
GO