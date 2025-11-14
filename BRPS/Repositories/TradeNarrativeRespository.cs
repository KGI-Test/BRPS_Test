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
    class TradeNarrativeRespository : RepositoryBase
    {
        private BRPSDbContext context = new BRPSDbContext();
        private const string EXCHANGE_DRIVER = "EXDR";
        private const string CHEQUE_COMMENT = "CHQC";
        private const string CHEQUE_NO = "CHQN";
        private const string EXTERNAL_NARRATIVE = "ENAR";
        private const string INTERNAL_COMMENT = "INTL";

        #region Deconstructure
        ~TradeNarrativeRespository()
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

            public List<Trade_NarrativeInfo> GetTradeNarrativeFromTE()
            {
                var ds = ExtractTradeNarrativeFromTE();

                if (ds == null || ds.Tables.Count == 0)
                {
                    return null;
                }

                var TradNar = new List<Trade_NarrativeInfo>();
                foreach (DataRow row in ds.Tables[0].Rows)
                {
                    var data = new Trade_NarrativeInfo()
                    {
                        Trade_No = Convert.ToInt32(row["record_no_wil"].ToString().TrimEnd()),
                        Narrative_Code = row["narrative_code_wil"].ToString().TrimEnd(),
                        Narrative = row["narrative_wil"].ToString().TrimEnd(),
                        Version_date = Convert.ToDateTime(row["version_date"]),
                        Version_no = Convert.ToInt32(row["version_no"])
                    };

                    TradNar.Add(data);
                }
                return TradNar;
            }

            public DataSet ExtractTradeNarrativeFromTE()
            {
                DataSet ds = null;
                using (var dao = new GenericDAO(new OleDbConnectionSettingsProvider()))
                {
                    var ssql = new StringBuilder("SELECT RN.record_no_wil, RN.narrative_code_wil, RN.narrative_wil, RN.version_date, RN.version_no FROM record_narrative_wil RN WHERE RN.version_date > DATEADD(month, -1, GETDATE())"); //Past One Month
                    ssql.AppendFormat(" and RN.narrative_code_wil in ('{0}','{1}','{2}','{3}','{4}') ", EXCHANGE_DRIVER, CHEQUE_COMMENT, CHEQUE_NO, EXTERNAL_NARRATIVE, INTERNAL_COMMENT);
                    
                    ds = dao.ExecuteQuery(ssql.ToString());
                }

                return ds;
            }

        #endregion

        public int InsertTradeNarrativeInfo(List<Trade_NarrativeInfo> TNarInfo, Int32 batchSize = 1000)
        {
            context.Database.ExecuteSqlCommand("Delete Trade_NarrativeInfo");
            return InsertEntities<Trade_NarrativeInfo>(context, TNarInfo, batchSize);
        }
    }
}