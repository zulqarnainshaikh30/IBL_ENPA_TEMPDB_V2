CREATE TABLE [dbo].[TempExceptionFinalStatusType] (
  [Entity_Key] [int] IDENTITY,
  [SourceAlt_Key] [int] NULL,
  [CustomerID] [varchar](50) NULL,
  [ACID] [varchar](20) NULL,
  [StatusType] [varchar](30) NULL,
  [StatusDate] [date] NULL,
  [Amount] [decimal](18, 2) NULL,
  [AuthorisationStatus] [varchar](5) NULL,
  [EffectiveFromTimeKey] [int] NULL,
  [EffectiveToTimeKey] [int] NULL,
  [CreatedBy] [varchar](100) NULL,
  [DateCreated] [datetime] NULL,
  [ModifyBy] [varchar](100) NULL,
  [DateModified] [date] NULL,
  [ApprovedBy] [varchar](100) NULL,
  [DateApproved] [date] NULL,
  [IsChanged] [char](1) NULL,
  [IS_ETL] [char](1) NULL,
  [AccountEntityId] [int] NULL
)
ON [PRIMARY]
GO