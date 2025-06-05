CREATE TABLE [dbo].[IRAC_INVT_BASIC_DET] (
  [Date_of_Data] [date] NULL,
  [Source_System_Name] [varchar](10) NULL,
  [Branch_Code] [varchar](10) NULL,
  [Issuer_ID] [varchar](50) NULL,
  [Investment_ID] [varchar](50) NULL,
  [ISIN] [varchar](50) NULL,
  [Instr_Type] [varchar](50) NULL,
  [Instr_Name] [varchar](50) NULL,
  [Currency] [varchar](10) NULL,
  [Investment_Nature] [varchar](50) NULL,
  [Sector] [varchar](50) NULL,
  [Industry] [varchar](50) NULL,
  [ExposureType] [varchar](50) NULL,
  [Security_Value] [varchar](50) NULL,
  [Maturity_Date] [date] NULL,
  [ReStructure_Date] [date] NULL
)
ON [PRIMARY]
GO