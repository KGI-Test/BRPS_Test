using BRPS.DAO;
using BRPS.Model;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BRPS.Repositories
{
    class HolidayRespository : RepositoryBase
    {
        private BRPSDbContext context = new BRPSDbContext();

        #region Deconstructure
        ~HolidayRespository()
        {
            try
            {
                context.Dispose();
            }
            catch (Exception)
            {
                ;
            }
        }
        #endregion

        #region extract holiday data from Gloss & populate holiday entities

            public DataSet ExtractHolidaysFromTE()
        {
            DataSet ds = null;
            using (var dao = new GenericDAO(new OleDbConnectionSettingsProvider()))
            {
                var ssql = new StringBuilder("SELECT holiday_id_p2k+CONVERT(VARCHAR(8),holiday_date_p2k,112) AS holiday_id_p2k,holiday_date_p2k,weekend_day_p2k,version_date,version_no FROM holiday_p2k WHERE holiday_date_p2k LIKE '%2016%'");

                ds = dao.ExecuteQuery(ssql.ToString());
            }

            return ds;
        }

            public List<Holiday> GetHolidaysListFromTE()
        {
            var ds = ExtractHolidaysFromTE();

            if (ds == null || ds.Tables.Count == 0)
            {
                return null;
            }

            var Hld = new List<Holiday>();
            foreach (DataRow row in ds.Tables[0].Rows)
            {
                var data = new Holiday()
                {
                    Holiday_Id = row["holiday_id_p2k"].ToString().TrimEnd(),
                    Holiday_Date = Convert.ToDateTime(row["holiday_date_p2k"].ToString().TrimEnd()),
                    Weekend_Day = Convert.ToInt32(row["weekend_day_p2k"].ToString().TrimEnd()),
                    Version_Date = Convert.ToDateTime(row["version_date"]),
                    Version_No = Convert.ToInt32(row["version_no"])
                };

                Hld.Add(data);
            }
            return Hld;
        }

        #endregion

        public int InsertHolidays(List<Holiday> Holdi, Int32 batchSize = 1000)
        {
            //context.Database.ExecuteSqlCommand("Delete Holidays");
            return InsertEntities<Holiday>(context, Holdi, batchSize);
        }
    }
}
