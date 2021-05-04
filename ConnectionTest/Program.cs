using System;
using System.Data;
using System.Data.SqlClient;
using NLog;

namespace ConnectionTest
{
	class Program
	{

		private static Logger logger = LogManager.GetCurrentClassLogger();

		static void Main(string[] args)
		{
			logger.Info("Testing Connection");
			if (!TestConnection()) { logger.Info("No Connection Made"); return; };

			getDistinct(2);
			getAMM(3);
			getmerge(4);
			getDelete(5, "AlternateCCN > 0");
			getDelete(6, "State = 'AZ'");
			getAMM(7);

			
			logger.Info("Completed Successfully");
		}

		private static void getDistinct(int Question)
		{
			int row = 1;
			logger.Debug($"Retrieving list of Distinct Cities for Question {Question}");
			string sql = $"Select Distinct City, State from StandardizedReadmissionRatioPayment2021";
			using SqlConnection conn = GetConnection();
			conn.Open();
			using SqlCommand cmd = new SqlCommand(sql, conn);
			SqlDataReader sdr = cmd.ExecuteReader();
			try
			{
				if (sdr.HasRows)
				{
					while (sdr.Read())
					{
						string Cities = Util.DataReaderUtil.GetSafeString(sdr, "City");
						string States = Util.DataReaderUtil.GetSafeString(sdr, "State");
						string datarow = $"{row}: {Cities}, {States}";
						logger.Debug(datarow);
						row = row + 1;
					}
				}
			}
			catch (Exception e)
			{
				logger.Error(e.StackTrace);
				return;
			}
		}

		private static void getAMM(int Question)
        {
			logger.Debug($"Calculating Average for Question {Question}");
			string sql = @$"Select AVG(Try_Cast(AchievementMeasureRatio as float)) AS AVG_ACHIEVE,
								   Max(Try_Cast(AchievementMeasureRatio as float)) AS Max_ACHIEVE,
								   Min(Try_Cast(AchievementMeasureRatio as float)) AS Min_ACHIEVE 
								from StandardizedReadmissionRatioPayment2021";
			using SqlConnection conn = GetConnection();
			conn.Open();
			using SqlCommand cmd = new SqlCommand(sql, conn);
			SqlDataReader sdr = cmd.ExecuteReader();
			try
			{
				if (sdr.HasRows)
				{
					while (sdr.Read())
					{
						double AVG = Util.DataReaderUtil.GetSafeDouble(sdr, "AVG_ACHIEVE");
						double MAX = Util.DataReaderUtil.GetSafeDouble(sdr, "Max_ACHIEVE");
						double MIN = Util.DataReaderUtil.GetSafeDouble(sdr, "Min_ACHIEVE");
						string datarow = $"Average Achievement: {AVG}\n\t\t\t\t\t\t\t\t\t  Max Achievement: {MAX}\n\t\t\t\t\t\t\t\t\t  Min Achievement: {MIN}";
						logger.Debug(datarow);
					}
				}
			}
			catch (Exception e)
			{
				logger.Error(e.StackTrace);
				return;
			}
		}

		private static void getmerge(int Question)
        {
			int row = 0;
			try
			{
				logger.Debug($"Beginning Merge of Data Sets for Question: {Question}");
				string sql = @$"MERGE [dbo].[StandardizedReadmissionRatioPayment2021] as t 
										USING [dbo].[Payment2021Temp] as s
										ON (s.CMSCertificationNumber = t.CMSCertificationNumber)
								WHEN MATCHED
										THEN UPDATE SET 
											t.AchievementMeasureRatio = s.AchievementMeasureRatio;
                    ";
				using SqlConnection conn = GetConnection();
				conn.Open();
				using SqlCommand cmd = new SqlCommand(sql, conn);
				int rowsaffected = cmd.ExecuteNonQuery();
				row = row + rowsaffected;
				logger.Debug($"Rows Affected: {row}");
				logger.Debug($"Completed Question {Question}, Moving on to Question {Question + 1}");
			}
			catch (Exception e)
			{
				logger.Error(e.StackTrace);
				return;
			}
		}

		private static void getDelete(int Question, string Target)
		{
			int row = 0;
			try
			{
				logger.Debug($"Beginning Removal of Data Containing {Target} from Data Set for Question: {Question}");
				string sql = @$"DELETE FROM [dbo].[StandardizedReadmissionRatioPayment2021]
								      WHERE {Target}
                    ";
				using SqlConnection conn = GetConnection();
				conn.Open();
				using SqlCommand cmd = new SqlCommand(sql, conn);
				int rowsaffected = cmd.ExecuteNonQuery();
				row = row + rowsaffected;
				logger.Debug($"Rows Affected: {row}");
				logger.Debug($"Completed Question {Question}, Moving on to Question {Question + 1}");
			}
			catch (Exception e)
			{
				logger.Error(e.StackTrace);
				return;
			}
		}

		private static bool TestConnection()
		{
			try
			{
				using SqlConnection conn = GetConnection();
				conn.Open();
				logger.Debug("Connected");
			}
			catch(Exception e)
            {
				logger.Error($"Not Connected {e.StackTrace}");
				return false;
			}
			return true;
		}

		private static SqlConnection GetConnection()
		{
			string _connstr = "Server=LAPTOP-EFG50VT7\\SQLEXPRESS;Database=Final_Exam;Trusted_Connection=True;MultipleActiveResultSets=true;Connection Timeout=60";
			SqlConnection Connection = null;
			try
			{
				Connection = new SqlConnection(_connstr);
			}
			catch (Exception e)
			{
				logger.Error(e.StackTrace);
			}
			return Connection;
		}
	}
}
