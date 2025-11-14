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
    class InstrumentRepository : RepositoryBase
    {
     
        public enum SecurityGroup
        {
            EQTY,
            WARR
        }

        private enum InstrClass
        {
            Domicile_Country = 17,
            Charge_Class = 1005,
            Instr_Settle = 1210,
            Security_Type = 2162,
            Margin_Category=2163,
            Product_Type = 2806,
        }

        private const string  PROPRIETARY_CODE = "SXSI";
        private const string  LOCAL_CODE = "SESC";
        private const string  ISIN_CODE = "ISIN";
        private const string  RIC_CODE = "RIC";
        private const string  BMBG_CODE = "BMBG";
        private const string  SG_CDP_CODE = "SCDP";
        private const string  SEDL_CODE = "SEDL";
        private const string COMM_CODE = "COMM";
        private const string SYMBOL_CODE = "SYMB";
        private const string GROUP_CODE = "IRO1";
        private const string LISTING_DATE = "LDAT";
        private const string DELISTING_DATE = "DLDT";

        private BRPSDbContext context = new BRPSDbContext();

        #region Deconstructure
        ~InstrumentRepository()
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

        #region extract instrument data from Gloss & populate instrument entities
        public DataSet ExtractInstrumentsFromTE()
        {
            var markets =new PartyRespository().GetAllMarkets();
            string marketFilter = "('" + string.Join("','", markets.Select(m => m.Market_Code)) + "','')";
            
            DataSet ds = null;
            using (var dao = new GenericDAO(new OleDbConnectionSettingsProvider()))
            {
                string filters = " where main_market_p2k in " + marketFilter;
                var ssql= new StringBuilder( "SELECT i.p2000_instr_ref_p2k, instr_group_p2k, instr_long_name_p2k, instr_short_name_p2k, main_market_p2k, denom_instr_p2k," ); 
                ssql.Append( " settle_instr_p2k, book_wil, price_decimal_place_p2k, price_divisor_wil, price_multiplier_wil, price_type_p2k, value_of_tick_wil,");
                ssql.Append( " max_price_movt_p2k, min_price_movt_p2k, min_tradeable_qty_p2k, qty_decimal_place_p2k, active_ind_p2k, screen_type_wil, sched_no_wil,");
                ssql.Append(" country_code_p2k, i.version_date, i.version_no, denom_qty_p2k=isnull((select denom_qty_p2k from denomination_p2k d where i.p2000_instr_ref_p2k=d.p2000_instr_ref_p2k),0) " );
                ssql.Append(" FROM instrument_p2k i ");
                ssql.Append( filters );

                ssql.Append("SELECT c.p2000_instr_ref_p2k, c.classification_type_p2k, c.class_p2k FROM instr_classification_p2k c INNER JOIN instrument_p2k i ");
                ssql.Append("ON c.p2000_instr_ref_p2k=i.p2000_instr_ref_p2k");
                ssql.Append(filters);

                ssql.Append("SELECT e.p2000_instr_ref_p2k, e.instr_code_type_p2k,e.ext_instr_ref_p2k FROM instr_ext_ref_p2k e INNER JOIN instrument_p2k i ");
                ssql.Append(" ON e.p2000_instr_ref_p2k=i.p2000_instr_ref_p2k");
                ssql.Append(filters);

                ssql.Append("SELECT w.p2000_instr_ref_p2k,expiry_date_wil,location_code_p2k, expiry_type_wil, settle_code_wil,call_put_type_ind_p2k,call_put_instr_p2k,");
                ssql.Append("ratio_attached_p2k, narrative_wil,last_notice_date_time_wil, payment_instr_p2k, w.price_type_p2k ");
                ssql.Append("FROM warrant_instr_wil w INNER JOIN instrument_p2k i ON w.p2000_instr_ref_p2k=i.p2000_instr_ref_p2k ");
                ssql.Append(filters);

                ssql.Append("SELECT i.p2000_instr_ref_p2k, d.date_type_wil, d.date_wil FROM instr_date_wil d INNER JOIN instrument_p2k i ");
                ssql.Append("ON d.p2000_instr_ref_p2k = i.p2000_instr_ref_p2k");
                ssql.Append(filters);

                ds = dao.ExecuteQuery(ssql.ToString());
            }

            ds.Relations.Add(new DataRelation("Classification", ds.Tables[0].Columns[0], ds.Tables[1].Columns[0]));
            ds.Relations.Add(new DataRelation("External_ref", ds.Tables[0].Columns[0], ds.Tables[2].Columns[0]));
            ds.Relations.Add(new DataRelation("Warrant", ds.Tables[0].Columns[0], ds.Tables[3].Columns[0]));
            ds.Relations.Add(new DataRelation("Date", ds.Tables[0].Columns[0], ds.Tables[4].Columns[0]));
            
            return ds;
        }

        public List<Instrument> GetInstrumentListFromTE()
        {
            var ds = ExtractInstrumentsFromTE();

            if (ds == null || ds.Tables.Count == 0)
            {
                return null;
            }

            var Instrs=new  List<Instrument>();
            foreach (DataRow row in ds.Tables[0].Rows)
            {
                var instr=new Instrument(){
                    Instr_Ref=row["p2000_instr_ref_p2k"].ToString().TrimEnd(),
                    Instr_Group = row["instr_group_p2k"].ToString().TrimEnd(),
                    Long_Name = row["instr_long_name_p2k"].ToString().TrimEnd(),
                    Short_Name = row["instr_short_name_p2k"].ToString().TrimEnd(),
                    Main_Market = row["main_market_p2k"].ToString().TrimEnd(),
                    Denom_Ccy = row["denom_instr_p2k"].ToString().TrimEnd(),
                    Denom_Qty = Convert.ToInt32(row["denom_qty_p2k"]),
                    Settle_Instr = row["settle_instr_p2k"].ToString().TrimEnd(),
                    Book = row["book_wil"].ToString().TrimEnd(),
                    Price_Decimal_Place=Convert.ToInt32(row["price_decimal_place_p2k"]),
                    Price_Divisor=Convert.ToDecimal(row["price_divisor_wil"]),
                    Price_Multiplier=Convert.ToDecimal(row["price_multiplier_wil"]),
                    Price_Type = row["price_type_p2k"].ToString().TrimEnd(),
                    Min_Tradeable_Qty=Convert.ToInt32(row["min_tradeable_qty_p2k"]),
                    Qty_Decimal_Place=Convert.ToInt32(row["qty_decimal_place_p2k"]),
                    Active_Ind = row["active_ind_p2k"].ToString().TrimEnd(),
                    Country = row["country_code_p2k"].ToString().TrimEnd(),                   
                    Version_Date=Convert.ToDateTime(row["version_date"]),
                    Version_No=Convert.ToInt32(row["version_no"])
                };

                PopulateWarrantInfo(row, instr);
                PopulateInstrClassification(row, instr);
                PopulateInstrExeternalRef(row, instr);
                PopulateInstrDate(row, instr);

                //instr.Listing_Date=new DateTime(1990,1,1);
                //instr.Delisting_Date=null;
                instr.Margin_Category=null;
                instr.Margin_Ratio= 0M;
                instr.Margin_Limit=0M;

                Instrs.Add(instr);
            }
            return Instrs;
        }

        private static void PopulateInstrDate(DataRow row, Instrument instr)
        {
            foreach (var refrow in row.GetChildRows("Date"))
            {
                var data = refrow["date_wil"].ToString().TrimEnd();
                switch (refrow["date_type_wil"].ToString())
                {
                    case LISTING_DATE:
                        instr.Listing_Date = Convert.ToDateTime(data);
                        break;
                    case DELISTING_DATE:
                        instr.Delisting_Date = Convert.ToDateTime(data);
                        break;
                    default:
                        instr.Listing_Date = new DateTime(1990, 1, 1);
                        instr.Delisting_Date = null;
                        break;
                }
            }
        }

        private static void PopulateInstrExeternalRef(DataRow row, Instrument instr)
        {
            foreach (var refrow in row.GetChildRows("External_ref"))
            {
                var data = refrow["ext_instr_ref_p2k"].ToString().TrimEnd();
                switch (refrow["instr_code_type_p2k"].ToString())
                {
                    case PROPRIETARY_CODE:
                        instr.Proprietary_Code = data;
                        break;
                    case LOCAL_CODE:
                        instr.Local_Code = data;
                        break;
                    case ISIN_CODE:
                        instr.ISIN_Code = data;
                        break;
                    case RIC_CODE:
                        instr.RIC_Code = data;
                        break;
                    case BMBG_CODE:
                        instr.BMBG_Code = data;
                        break;
                    case SG_CDP_CODE:
                        instr.SG_CDP_Code = data;
                        break;
                    case SEDL_CODE:
                        instr.SEDL_Code = data;
                        break;
                    case COMM_CODE:
                        instr.COMM_Code = data;
                        break;
                    case SYMBOL_CODE:
                        instr.Symbol_Code = data;
                        break;
                    case GROUP_CODE:
                        instr.Group_Code = data;
                        break;
                    default:
                        break;
                }
            }
        }

        private static void PopulateInstrClassification(DataRow row, Instrument instr)
        {
            foreach (var clsrow in row.GetChildRows("Classification"))
            {
                var data = clsrow["class_p2k"].ToString().TrimEnd();
                switch ((InstrClass)Convert.ToInt32(clsrow["classification_type_p2k"]))
                {
                    case InstrClass.Domicile_Country:
                        instr.Domicile_Country = data;
                        break;
                    case InstrClass.Charge_Class:
                        //instr.Charge_Class=data;
                        break;
                    case InstrClass.Product_Type:
                        instr.Product_Type = clsrow["class_p2k"].ToString();
                        break;
                    case InstrClass.Security_Type:
                        instr.Security_Type = data;
                        break;
                    case InstrClass.Margin_Category:
                        instr.Margin_Category = data;
                        break;
                    default:
                        break;
                }
            }
        }

        private static void PopulateWarrantInfo(DataRow row, Instrument instr)
        {
            Warrant warr = null;
            foreach (var warrrow in row.GetChildRows("Warrant"))
            {
                warr = new Warrant();
                instr.Warrant = warr;
                warr.Instrument = instr;
                warr.Instr_Ref = instr.Instr_Ref;
                warr.Expiry_Date = Convert.ToDateTime(warrrow["expiry_date_wil"]);
                warr.Location = warrrow["location_code_p2k"].ToString().TrimEnd();
                warr.Warrant_Type = "EQTY";
                warr.Price_Type = warrrow["price_type_p2k"].ToString().TrimEnd();
                warr.Settle_Type = warrrow["settle_code_wil"].ToString().TrimEnd();
                warr.Payment_Curr = warrrow["payment_instr_p2k"].ToString().TrimEnd();
                warr.Call_Put_Ind = warrrow["call_put_type_ind_p2k"].ToString().TrimEnd();
                warr.Strike_Price = 0m;
                warr.Last_Notice_Date = Convert.ToDateTime(warrrow["last_notice_date_time_wil"]);
                warr.Ratio = Convert.ToDecimal(warrrow["ratio_attached_p2k"]);
                warr.Delivery_Date = Convert.ToDateTime(warrrow["expiry_date_wil"]);
                warr.Underlying_Instr = ""; ;
                warr.Separation_Date = null;
                warr.Parent_Code = warrrow["call_put_instr_p2k"].ToString().TrimEnd();
            }
        }

        #endregion

        public int InsertInstruments(List<Instrument> Instruments, Int32 batchSize=1000)
        {
            context.Database.ExecuteSqlCommand("Delete Instrument");
            return InsertEntities<Instrument>(context,Instruments, batchSize);          
        }
    }
    
}
