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
    class ExchangeRateRespository : RepositoryBase
    {
        private BRPSDbContext context = new BRPSDbContext();

        #region Deconstructure
            ~ExchangeRateRespository()
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

        #region Extract Exchange Rate from Gloss & populate Exchange Rate entities

            public DataSet ExtractExchangeRateFromTE()
            {
                DataSet ds = null;
                string todaydate = Convert.ToString(DateTime.Today);
                using (var dao = new GenericDAO(new OleDbConnectionSettingsProvider()))
                {
                    DateTime dt = new DateTime(2008, 3, 9, 16, 5, 7, 123);
                    var ssql = new StringBuilder("SELECT A.ccy_ord_wil AS CCYPairID,A.p2000_instr_ref_p2k AS Instr_Ref,A.quote_instr_p2k AS Quote_Instr_Ref,A.market_mid_price_p2k AS Mid_Price,A.market_bid_price_p2k AS Bid_Price,A.market_offer_price_p2k AS Ask_Price,A.price_date_time_p2k AS Price_DateTime FROM instr_price_p2k A, instrument_p2k B WHERE " +
                                                 "B.instr_group_p2k = 'CURR' AND A.p2000_instr_ref_p2k = B.p2000_instr_ref_p2k AND A.quote_instr_p2k = 'SGD' AND A.price_date_time_p2k >= '" + String.Format("{0:d/M/yyyy}", DateTime.Today) + "'");

                    ds = dao.ExecuteQuery(ssql.ToString());
                }

                return ds;
            }

            public List<ExchangeRate> GetExchangeRateListFromTE()
            {
                var ds = ExtractExchangeRateFromTE();

                if (ds == null || ds.Tables.Count == 0)
                {
                    return null;
                }

                var ExR = new List<ExchangeRate>();
                foreach (DataRow row in ds.Tables[0].Rows)
                {
                    var data = new ExchangeRate()
                    {
                        CCYPairID = row["CCYPairID"].ToString().TrimEnd(),
                        Instr_Ref = row["Instr_Ref"].ToString().TrimEnd(),
                        Quote_Instr_Ref = row["Quote_Instr_Ref"].ToString().TrimEnd(),
                        Mid_Price = Convert.ToDecimal(row["Mid_Price"].ToString().TrimEnd()),
                        Bid_Price = Convert.ToDecimal(row["Bid_Price"]),
                        Ask_Price = Convert.ToDecimal(row["Ask_Price"]),
                        Price_DateTime = Convert.ToDateTime(row["Price_DateTime"])
                    };

                    ExR.Add(data);
                }
                return ExR;
            }

        #endregion

        public int InsertExchangeRate(List<ExchangeRate> ExRa, Int32 batchSize = 1000)
        {
            //context.Database.ExecuteSqlCommand("Delete Holidays");
            return InsertEntities<ExchangeRate>(context, ExRa, batchSize);
        }
    }
}
