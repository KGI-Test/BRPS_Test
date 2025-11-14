using BRPS.DAO;
using BRPS.Model;
using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


namespace BRPS.Repositories
{
    class InstrumentPriceRepository : RepositoryBase
    {
        private BRPSDbContext context = new BRPSDbContext();

        #region Deconstructure
        ~InstrumentPriceRepository()
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

            public List<Instrument_Price> GetInstPriceFromTE()
            {
                var ds = ExtractInstPriceFromTE();

                if (ds == null || ds.Tables.Count == 0)
                {
                    return null;
                }

                var TradNar = new List<Instrument_Price>();
                foreach (DataRow row in ds.Tables[0].Rows)
                {
                    var data = new Instrument_Price()
                    {
                        Instr_Ref = row["p2000_instr_ref_p2k"].ToString().TrimEnd(),
                        Close_Price = Convert.ToDouble(row["market_mid_price_p2k"].ToString().TrimEnd()),
                        Price_date = Convert.ToDateTime(row["price_date_time_p2k"]),
                        Version_date = Convert.ToDateTime(row["version_date"]),
                        Version_no = Convert.ToInt32(row["version_no"])
                    };

                    TradNar.Add(data);
                }
                return TradNar;
            }
            
            // Modified on 28 Jan 2019, By Jay due to GLOSS is having one price feed where Reuters. Removed AgencyBook from party filter.
            public DataSet ExtractInstPriceFromTE()
            {
                DataSet ds = null;
                using (var dao = new GenericDAO(new OleDbConnectionSettingsProvider()))
                {

                    var ssql = new StringBuilder();

                    if (DateTime.Now.DayOfWeek.ToString() == "Monday")
                    {
                        ssql = new StringBuilder("SELECT P.p2000_instr_ref_p2k,P.market_mid_price_p2k,P.price_date_time_p2k,P.version_date,P.version_no,I.main_market_p2k FROM instr_price_p2k AS P INNER JOIN instrument_p2k AS I ON P.p2000_instr_ref_p2k = I.p2000_instr_ref_p2k WHERE I.main_market_p2k NOT IN ('SG EQTY MKT', 'SG EQTY MKT1', 'FOREX') AND P.price_date_time_p2k = dateadd(day,0,convert(Date, getdate(), 365)) Union all SELECT P.p2000_instr_ref_p2k,P.market_mid_price_p2k,P.price_date_time_p2k,P.version_date,P.version_no,I.main_market_p2k FROM instr_price_p2k AS P INNER JOIN instrument_p2k AS I ON P.p2000_instr_ref_p2k = I.p2000_instr_ref_p2k WHERE I.main_market_p2k IN ('US EQTY MKT','GB EQTY MKT','NYSE EQ ITFT','NYSE EQ FRFT','NL EQTY MKT','FR EQTY MKT','ES EQTY MKT') AND P.price_date_time_p2k = dateadd(day,-3,convert(Date, getdate(), 365))");
                    }
                    else
                    {
                        ssql = new StringBuilder("SELECT P.p2000_instr_ref_p2k,P.market_mid_price_p2k,P.price_date_time_p2k,P.version_date,P.version_no,I.main_market_p2k FROM instr_price_p2k AS P INNER JOIN instrument_p2k AS I ON P.p2000_instr_ref_p2k = I.p2000_instr_ref_p2k WHERE I.main_market_p2k NOT IN ('SG EQTY MKT', 'SG EQTY MKT1', 'FOREX') AND P.price_date_time_p2k = dateadd(day,0,convert(Date, getdate(), 365)) Union all SELECT P.p2000_instr_ref_p2k,P.market_mid_price_p2k,P.price_date_time_p2k,P.version_date,P.version_no,I.main_market_p2k FROM instr_price_p2k AS P INNER JOIN instrument_p2k AS I ON P.p2000_instr_ref_p2k = I.p2000_instr_ref_p2k WHERE I.main_market_p2k IN ('US EQTY MKT','GB EQTY MKT','NYSE EQ ITFT','NYSE EQ FRFT','NL EQTY MKT','FR EQTY MKT','ES EQTY MKT') AND P.price_date_time_p2k = dateadd(day,-1,convert(Date, getdate(), 365))");
                    }
                    
                    ds = dao.ExecuteQuery(ssql.ToString());
                }

                return ds;
            }

            public List<Instrument_Price> GetInstPriceFXFromTE()
            {
                var ds = ExtractInstPriceFXFromTE();

                if (ds == null || ds.Tables.Count == 0)
                {
                    return null;
                }

                var TradNar = new List<Instrument_Price>();
                foreach (DataRow row in ds.Tables[0].Rows)
                {
                    var data = new Instrument_Price()
                    {
                        Instr_Ref = row["p2000_instr_ref_p2k"].ToString().TrimEnd(),
                        Close_Price = Convert.ToDouble(row["market_mid_price_p2k"].ToString().TrimEnd()),
                        Price_date = Convert.ToDateTime(row["price_date_time_p2k"]),
                        Version_date = Convert.ToDateTime(row["version_date"]),
                        Version_no = Convert.ToInt32(row["version_no"])
                    };

                    TradNar.Add(data);
                }
                return TradNar;
            }

            public DataSet ExtractInstPriceFXFromTE()
            {
                DataSet ds = null;
                using (var dao = new GenericDAO(new OleDbConnectionSettingsProvider()))
                {
                    // Current LDP is from SGX data file(1 day delay), Hence price_date_time_p2k = dateadd(day,-1,convert(Date, getdate(), 365))
                    //DateTime dateValue = DateTime.Parse(DateTime.Now.ToString(), CultureInfo.InvariantCulture);
                    //DateTimeOffset dateOffsetValue = new DateTimeOffset(dateValue,
                    //                             TimeZoneInfo.Local.GetUtcOffset(dateValue));
                    var ssql = new StringBuilder();

                    //if (DateTime.Now.DayOfWeek.ToString() == "Monday")
                    //{
                     //   ssql.Append("SELECT p2000_instr_ref_p2k,market_mid_price_p2k,price_date_time_p2k,version_date,version_no FROM instr_price_p2k WHERE quote_instr_p2k = 'SGD' and price_date_time_p2k = dateadd(day,-3,convert(Date, getdate(), 365))");
                    //}
                    //else
                    //{
                        ssql.Append("SELECT p2000_instr_ref_p2k,market_mid_price_p2k,price_date_time_p2k,version_date,version_no FROM instr_price_p2k WHERE quote_instr_p2k = 'SGD' and price_date_time_p2k = dateadd(day,0,convert(Date, getdate(), 365))");
                    //}

                    ds = dao.ExecuteQuery(ssql.ToString());
                }

                return ds;
            }

        #endregion

       public int InsertInstrumentPxInfo(List<Instrument_Price> InstrPxInfo, Int32 batchSize = 1000)
       {
           context.Database.ExecuteSqlCommand("insert into dbo.Instrument_Price_archive select * from dbo.Instrument_Price;");
           return InsertEntities<Instrument_Price>(context, InstrPxInfo, batchSize);
       }

       public int InsertInstrumentFXPxInfo(List<Instrument_Price> InstrPxInfo, Int32 batchSize = 1000)
       {
           context.Database.ExecuteSqlCommand("insert into dbo.Instrument_Price_archive select * from dbo.Instrument_Price;");
           context.Database.ExecuteSqlCommand("Delete Instrument_Price;");
           context.Database.ExecuteSqlCommand("DBCC CHECKIDENT (Instrument_Price, RESEED, 0);");
           return InsertEntities<Instrument_Price>(context, InstrPxInfo, batchSize);
       }

       public void Update_Instrument_ClosePrice()
       {
           context.Database.ExecuteSqlCommand("exec dbo.Update_Instrument_ClosePrice");
       }
    }
}
