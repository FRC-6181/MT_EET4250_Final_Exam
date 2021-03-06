USE [Final_Exam]
GO
/****** Object:  StoredProcedure [dbo].[MergeData]    Script Date: 5/4/2021 6:10:13 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
ALTER PROCEDURE [dbo].[MergeData] 
	-- Add the parameters for the stored procedure here
	
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

	MERGE [dbo].[StandardizedReadmissionRatioPayment2021] as t 
		USING [dbo].[Payment2021Temp] as s
		ON (s.CMSCertificationNumber = t.CMSCertificationNumber)
	WHEN MATCHED
		THEN UPDATE SET 
			t.AchievementMeasureRatio = s.AchievementMeasureRatio;

    -- Insert statements for procedure here
	
END
