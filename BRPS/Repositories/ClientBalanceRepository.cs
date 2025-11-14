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
    class ClientBalanceRepository : RepositoryBase
    {
        private BRPSDbContext context = new BRPSDbContext();
        private const string PERIOD_TYPE = "DALY"; // Custody
        private const string COMPANY = "CMP1"; // Settle Date
        private const string BALANCE_CODE = "CUST"; // Custody
        //private const string COLUMN_CODE = "SFSD"; // Settle Date
        private const string MARKET_CODE = "FOREX"; // Only Currency
        private const string COLUMN_CODE1 = "SFSD"; // Settle Date
        private const string COLUMN_CODE2 = "CAPC"; // Corporate Action

       #region Deconstructure
        ~ClientBalanceRepository()
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

        #region 

            public DataSet ExtractClientCashBalanceFromTE()
            {
                DataSet ds = null;
                using (var dao = new GenericDAO(new OleDbConnectionSettingsProvider()))
                {
                    var ssql = new StringBuilder("SELECT l.party_ref_p2k, l.p2000_instr_ref_p2k, l.quantity_wil, l.occur_date_wil FROM ldg_balance_view AS l JOIN instrument_p2k AS s ON s.p2000_instr_ref_p2k = l.p2000_instr_ref_p2k");
                    ssql.AppendFormat(" where l.period_type_wil in ('{0}') ", PERIOD_TYPE);
                    ssql.AppendFormat(" and l.company_p2k in ('{0}') ", COMPANY);
                    ssql.AppendFormat(" and l.balance_code_wil in ('{0}') ", BALANCE_CODE);
                    ssql.AppendFormat(" and l.column_code_wil in ('{0}') ", COLUMN_CODE1);
                    ssql.AppendFormat(" and s.main_market_p2k in ('{0}') ", MARKET_CODE);

                    ds = dao.ExecuteQuery(ssql.ToString());
                }

                return ds;
            }

            public DataSet ExtractClientStockBalanceFromTE()
            {
                DataSet ds = null;
                using (var dao = new GenericDAO(new OleDbConnectionSettingsProvider()))
                {
                    var ssql = new StringBuilder("SELECT l.party_ref_p2k, l.p2000_instr_ref_p2k, l.quantity_wil, l.occur_date_wil,l.column_code_wil FROM ldg_balance_view AS l JOIN instrument_p2k AS s ON s.p2000_instr_ref_p2k = l.p2000_instr_ref_p2k");
                    ssql.AppendFormat(" where l.period_type_wil in ('{0}') ", PERIOD_TYPE);
                    ssql.AppendFormat(" and l.company_p2k in ('{0}') ", COMPANY);
                    ssql.AppendFormat(" and l.balance_code_wil in ('{0}') ", BALANCE_CODE);
                    //ssql.AppendFormat(" and l.column_code_wil in ('{0}') ", COLUMN_CODE);
                    ssql.AppendFormat(" and l.column_code_wil in ('{0}','{1}') ", COLUMN_CODE1, COLUMN_CODE2);
                    ssql.AppendFormat(" and s.main_market_p2k not in ('{0}') ", MARKET_CODE);

                    ds = dao.ExecuteQuery(ssql.ToString());
                }

                return ds;
            }

            //public List<Client_CashBalance> GetClientCashBalanceFromTE()
            //{
            //    var ds = ExtractClientCashBalanceFromTE();

            //    if (ds == null || ds.Tables.Count == 0)
            //    {
            //        return null;
            //    }

            //    var CliBal = new List<Client_CashBalance>();
            //    foreach (DataRow row in ds.Tables[0].Rows)
            //    {
            //        var data = new Client_CashBalance()
            //        {
            //            Client_Account_Number = row["party_ref_p2k"].ToString().TrimEnd(),
            //            Ccy = row["p2000_instr_ref_p2k"].ToString().TrimEnd(),
            //            Amount = Convert.ToDecimal(row["quantity_wil"].ToString().TrimEnd()),
            //            Update_Date = Convert.ToDateTime(row["occur_date_wil"].ToString().TrimEnd())
            //        };

            //        CliBal.Add(data);
            //    }

            //    return CliBal;
            //}

            // Modified on 28 Jan 2019, By Jay due to GLOSS l.quantity_wil from ldg_balance_view(GLOSS) value changed.
            public List<Client_CashBalance> GetClientCashBalanceFromTE()
            {
                var ds = ExtractClientCashBalanceFromTE();

                if (ds == null || ds.Tables.Count == 0)
                {
                    return null;
                }

                var CliBal = new List<Client_CashBalance>();
                foreach (DataRow row in ds.Tables[0].Rows)
                {
                    var data = new Client_CashBalance()
                    {
                        Client_Account_Number = row["party_ref_p2k"].ToString().TrimEnd(),
                        Ccy = row["p2000_instr_ref_p2k"].ToString().TrimEnd(),
                        Amount = Convert.ToDecimal(row["quantity_wil"]), // removed => ToString().TrimEnd()
                        Update_Date = Convert.ToDateTime(row["occur_date_wil"].ToString().TrimEnd())
                    };

                    CliBal.Add(data);
                }

                return CliBal;
            }

            public List<Client_StockBalance> GetClientStockBalanceFromTE()
            {                
                var ds = ExtractClientStockBalanceFromTE();

                if (ds == null || ds.Tables.Count == 0)
                {
                    return null;
                }

                var CliBal = new List<Client_StockBalance>();
                foreach (DataRow row in ds.Tables[0].Rows)
                {
                    try
                    {                        
                        string strAmt = row["quantity_wil"].ToString().TrimEnd();
                        string strAmt2 = "0";
                        decimal Amt = 0;
                        decimal Amt1 = 0;                     

                        if (decimal.TryParse(strAmt, System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out Amt))
                        {
                            Amt1 = Amt;
                        }

                        strAmt2 = string.Format("{0:0.0000000000#}", Amt1);
                    
                        var data = new Client_StockBalance()
                        {
                            Client_Account_Number = row["party_ref_p2k"].ToString().TrimEnd(),
                            Ccy = row["p2000_instr_ref_p2k"].ToString().TrimEnd(),
                            //Amount = Convert.ToDecimal(row["quantity_wil"]), //.ToString().TrimEnd()),
                            Amount = Convert.ToDecimal(strAmt2), //.ToString().TrimEnd()),
                            Update_Date = Convert.ToDateTime(row["occur_date_wil"].ToString().TrimEnd()),
                            Type = row["column_code_wil"].ToString().TrimEnd()
                        };
                    
                        CliBal.Add(data);                    
                    }
                    catch (Exception Ex)
                    {
                        String str = Ex.Message.ToString();
                    }

                }

                return CliBal;
            }

        #endregion

        public int InsertClientCashBalanceInfo(List<Client_CashBalance> TNarInfo, Int32 batchSize = 1000)
        {
            context.Database.ExecuteSqlCommand("Delete Client_CashBalance");
            return InsertEntities<Client_CashBalance>(context, TNarInfo, batchSize);
        }

        public int InsertClientStockBalanceInfo(List<Client_StockBalance> TNarInfo, Int32 batchSize = 1000)
        {
            context.Database.ExecuteSqlCommand("Delete Client_StockBalance");
            return InsertEntities<Client_StockBalance>(context, TNarInfo, batchSize);
        }
    }
}