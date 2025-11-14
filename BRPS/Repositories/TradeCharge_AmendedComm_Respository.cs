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
    class TradeCharge_AmendedComm_Respository  : RepositoryBase
    {
        private BRPSDbContext context = new BRPSDbContext();
        private const string COMMISSION = "COMM";
        private const string TRADE_STATUS = "X";

        #region Deconstructure
            ~TradeCharge_AmendedComm_Respository()
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

        #region Extract

            public DataSet ExtractTradeCharge_AmendedComm_FromTE()
            {
                DataSet ds = null;
                using (var dao = new GenericDAO(new OleDbConnectionSettingsProvider()))
                {
                    var ssql = new StringBuilder("SELECT T.secondary_party_p2k,T.p2000_trade_ref_p2k,C.record_no_wil,C.charge_levy_qty_p2k,T.trade_date_wil,T.entry_date_time_p2k FROM charge_levy_p2k C INNER JOIN trade_p2k T ON C.record_no_wil=T.trade_no_wil");
                    ssql.AppendFormat(" where C.charge_levy_type_p2k in ('{0}') ", COMMISSION);
                    ssql.AppendFormat(" and T.trade_status_p2k in ('{0}') ", TRADE_STATUS);
                    //ssql.AppendFormat(" and T.version_date > DATEADD(month, -1, GETDATE())"); //Past One Month
                    //ssql.AppendFormat(" and T.trade_date_wil >= dateadd(day,-20,convert(Date, getdate(), 365))"); //Past 20 Days
                    ssql.AppendFormat(" and T.version_date >= dateadd(day,-20,convert(Date, getdate(), 365))"); //Past One Month
                    ds = dao.ExecuteQuery(ssql.ToString());
                }

                return ds;
            }
        
        #endregion

        #region GetFromTE
            public List<TradeCharge_AmendedComm> GetTradeCharge_AmendedComm_FromTE()
            {
                var ds = ExtractTradeCharge_AmendedComm_FromTE();

                if (ds == null || ds.Tables.Count == 0)
                {
                    return null;
                }

                var CliBal = new List<TradeCharge_AmendedComm>();
                foreach (DataRow row in ds.Tables[0].Rows)
                {
                    var data = new TradeCharge_AmendedComm()
                    {
                        Client_Number = row["secondary_party_p2k"].ToString().TrimEnd(),
                        Trade_Ref = row["p2000_trade_ref_p2k"].ToString().TrimEnd(),
                        Trade_No = Convert.ToInt32(row["record_no_wil"].ToString().TrimEnd()),
                        Commission = Convert.ToDecimal(row["charge_levy_qty_p2k"].ToString().TrimEnd()),
                        Trade_Date = Convert.ToDateTime(row["trade_date_wil"].ToString().TrimEnd()),
                        Created_Date = Convert.ToDateTime(row["entry_date_time_p2k"].ToString().TrimEnd())
                    };

                    CliBal.Add(data);
                }

                return CliBal;
            }
        #endregion

        #region
            public int InsertExtractTradeCharge_AmendedComm_FromTE(List<TradeCharge_AmendedComm> TNarInfo, Int32 batchSize = 1000)
            {
                context.Database.ExecuteSqlCommand("Delete TradeCharge_AmendedComm");
                return InsertEntities<TradeCharge_AmendedComm>(context, TNarInfo, batchSize);
            }
       #endregion
    }
}