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
    class Trade_Status_Respository : RepositoryBase
    {
        #region const variables definition

            private const string COMMISSION = "COMM";
            private const string HONG_KONG_STAMP_DUTY = "HKSD";
            private const string HONG_KONG_TRADING_FEE = "HKTF";
            private const string HONG_KONG_TRANSACTION_LEVY = "HKTL";
            private const string PRIMARY_QUANTITY = "PQTY";
            private const string PTM_LEVY = "PTM";
            private const string SETTLEMENT_FEE = "SETT";
            private const string SG_GST_ON_COMMISSION = "SG01";
            private const string SG_GST_ON_TRADING_FEE = "SG02";
            private const string SG_GST_ON_CLEARING_FEE = "SG03";
            private const string SINGAPORE_ACCESS_FEE = "SGAF";
            private const string SGX_CLEARING_FEE = "SGCL";
            private const string SG_GST_ON_COMMISSION_SGD = "SL01";
            private const string SG_GST_ON_TRADING_FEE_SGD = "SL02";
            private const string SG_GST_ON_CLEARING_FEE_SGD = "SL03";
            private const string NET_CONSIDERATION = "SQTY";
            private const string BLOOMBERG_PRINCIPLE = "BBPN";
            private const string BPS_BOOK_NET_AMOUNT_GROUP = "BNBG";
            private const string BPS_PRINCIPAL = "BPSP";
            private const string COMMISSION_GROUP = "COMG";
            private const string TRADE_PRINCIPLE_COMPARISION = "CPRN";
            private const string CREST_Stampable_Consideration = "CSCN";
            private const string PRINCIPAL_IN_DENOMINATION_CURRENCY = "DPRN";
            private const string SETTLE_VALUE_IN_DENOMINATION_CURRENCY = "DSQT";
            private const string GPF_FEES_EXPANSION_GROUP = "GPFE";
            private const string UNADJUSTED_PRINCIPAL = "OPRN";
            private const string PRINCIPAL_ACCRUED_SETTLE_CURRENCY = "PASC";
            private const string PRINCIPAL_IN_BASE_CURRENCY_SIGNED = "PBAS";
            private const string PRIMARY_TRADE_QUANTITY = "PQTY";
            private const string PTM_Base_Amount = "PTMB";
            private const string PRINCIPAL_IN_DENOMINATED_CURRENCY_SIGNED = "PVAL";
            private const string NET_CONSIDERATION_IN_BASE_CURRENCY_SIGNED = "SBAS";
            private const string PRINCIPAL_IN_SETTLE_CURRENCY = "SPRN";
            private const string NET_CONSIDERATION_IN_SETTLE_CURRENCY = "SQTY";
            private const string PRINCIPAL_IN_TRADE_CURRENCY = "TPRN";
            private const string TOTAL_SG_GST_SGD = "TSGL";
            private const string TOTAL_SG_GST = "TSGT";
            private const string SETTLE_VALUE_IN_TRADED_CURRENCY = "TSQT";
            private const string CONTRA_TRADE_REFERENCE = "CGTR";
            private const string CONTRA_SETTLEMENT = "CONT";
            private const string CONTRA_SPLIT_TRANSFORM = "CNTR";
            private const string CONTRA_TRANSFORM = "CONT";
            private const string ALLOCATION_PROCESSING_INDICATOR = "ALOC";

            private const string PRIMARY_SETTLEMENT = "PSET";
            private const string PRIMARY_SIDE_1 = "PTR1";
            private const string PRIMARY_SIDE_2 = "PTR2";
            private const string SECONDARY_SETTLEMENT = "SSET";
            private const string SECONDARY_SIDE_1 = "STR1";
            private const string SECONDARY_SIDE_2 = "STR2";

            private const string PSMS_SOURCE = "PSMS";
            private const string SG_EXTERNAL_REFERENCE = "SGXR";
            private const string STOCK_INSTRUCTION_REFERENCE_DELIVER = "STKD";
            private const string STOCK_INSTRUCTION_REFERENCE_RECEIVE = "STKR";
            private const string SGX_CONTRACT_NO = "SXCN";
            private const string SGX_CAS_REFERENCE_NUMBER = "SXCR";
            private const string TRADE_REFERENCE = "TRDE";
            private const string FRONT_OFFICE_REFERENCE_UNIQUE = "TREF";
            private const string TRANSACTION_REFERENCE = "TRID";
            private const string GLOSS_TRANSACTION_NUMBER = "TRNO";
            private const string M515_TRADE_REFERENCE = "TRRF";
            private const string WITH_REFERENCE = "WITH";
            private const string EXECUTION_NUMBER = "EXNO";

            private const string TRANSACTION_SETTLEMENT_TYPE = "TSET";
            private const string SUB_TRANSACTION_TYPE = "SUBT";
            private const string CASH_PAYMENT_METHOD = "CSMT";
            private const string CASH_PAYMENT_STATUS_DRIVER = "CSST";
            private const string TRADE_CHANNAL_DRIVER = "TRDC";

            private const string NARRATIVE_CODE = "EXDR";
            private const string DEPOT_ALIAS_EPS = "CPTY EPSG";
            private const string EXEC_SALESPERSON = "SAL2";

        #endregion

        #region Deconstructure
            ~Trade_Status_Respository()
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

        private enum TradeType
             {
                CAGS,
                FREC
             }

        private enum OperationType
            {
                CBUY,
                CSEL,
                BUY,
                SELL,
                DEL,
                REC
            }

        private BRPSDbContext context = new BRPSDbContext();

        public int InsertTrades(List<Trade_Status> trades, DateTime RunDate, Int32 batchNos = 100)
        {
            //string ssql = string.Format("delete Trade_Status where convert(char(8),Business_Date,112)='{0:yyyyMMdd}'", RunDate);
            string ssql = string.Format("delete from Trade_Status");
            context.Database.ExecuteSqlCommand(ssql);
            return InsertEntities<Trade_Status>(context, trades);
        }

        #region extract trades from Gloss popultate trade entities

            public DataSet ExtractTradesFromTE(DateTime bizDate)
            {
                var markets = new PartyRespository().GetAllMarkets();
                var marketFilter = " and market_wil in ('" + string.Join("','", markets.Select(m => m.Market_Code)) + "',' ')";

                DataSet ds = null;
                using (var dao = new GenericDAO(new OleDbConnectionSettingsProvider()))
                {
                    string filters = string.Format(" WHERE T.trade_type_wil IN ('{0}','{1}') AND T.trade_status_p2k IN ('A','N') {2} AND T.version_date > DATEADD(month, -1, GETDATE())",
                           TradeType.CAGS, TradeType.FREC, marketFilter);

                    var ssql = new StringBuilder("SELECT T.trade_no_wil,  p2000_trade_ref_p2k, trade_version_p2k, trade_status_p2k, entry_date_time_p2k, amend_date_wil,");
                    ssql.Append("trade_type_wil, proc_type_wil, operation_type_p2k, market_wil, trade_date_wil,  value_date_p2k,  primary_book_wil, ");
                    ssql.Append("primary_instr_p2k, primary_qty_p2k, secondary_instr_p2k,  price_decimal_p2k, price_type_p2k, mult_div_ind_p2k, factor_wil, ");
                    ssql.Append("secondary_party_p2k, settle_code_wil, settle_type_wil, settle_instr_p2k, settle_qty_p2k, trades_with_accrued_wil, accrued_interest_days_p2k, ");
                    ssql.Append("accrued_interest_rate_p2k, secondary_qty_p2k, secondary_instr_x_rate_p2k, secondary_instr_xri_p2k, accrued_interest_instr_p2k, ");
                    ssql.Append("accrued_interest_qty_p2k, accrued_interest_x_rate_p2k, accrued_interest_xri_p2k, settle_book_wil, driver_code_wil, maturity_date_p2k, ");
                    ssql.AppendFormat("Contra_Ref= (Select ext_trade_ref_wil from trade_ext_ref_wil E where T.trade_no_wil = E.record_no_wil and E.split_no_wil = 0 and E.trade_ref_type_wil='{0}') ", CONTRA_TRADE_REFERENCE);
                    ssql.Append(",proc_priority_wil, sched_no_wil, origin_wil, version_date, version_program, version_no, version_user, version_host ");
                    ssql.Append("FROM trade_p2k T ");
                    ssql.Append(filters);

                    //Un-use Query
                    //ssql.Append("SELECT C.record_no_wil, amount_type_wil, charge_levy_type_p2k,  charge_levy_instr_p2k, charge_discount_wil, leviable_instr_p2k, leviable_qty_p2k, ");
                    //ssql.Append("charge_levy_qty_p2k, charge_levy_rate_p2k, rate_type_wil FROM charge_levy_p2k C INNER JOIN trade_p2k T ON C.record_no_wil=T.trade_no_wil ");
                    //ssql.Append(filters);

                    ssql.Append("SELECT S.trade_no_wil, delivery_type_wil, settle_event_instr_p2k, settle_terms_wil, original_qty_p2k, open_qty_p2k,CASE WHEN S.settle_date_p2k <> S.version_date THEN S.version_date else S.settle_date_p2k END AS Settled_date, del_rec_ind_p2k, ");
                    ssql.Append("S.settle_status_p2k FROM settle_event_p2k S INNER JOIN trade_p2k T ON S.trade_no_wil=T.trade_no_wil ");
                    ssql.Append(filters);

                    //Optimized Query
                    //var ssql = new StringBuilder("SELECT b.trade_no_wil, b.delivery_type_wil, b.settle_event_instr_p2k, b.settle_terms_wil, b.original_qty_p2k,b.open_qty_p2k, " + 
                    //            "b.Settled_date,b.del_rec_ind_p2k, b.settle_status_p2k FROM(SELECT a.*, CASE WHEN a.settle_status_p2k='Y' then dateadd(mm, -2, getdate()) ELSE a.Settled_date END AS filter_date FROM " +
                    //            "(SELECT S.trade_no_wil, delivery_type_wil, settle_event_instr_p2k, settle_terms_wil, original_qty_p2k,open_qty_p2k,CASE WHEN S.settle_date_p2k <> S.version_date THEN S.version_date else S.settle_date_p2k END AS Settled_date, del_rec_ind_p2k, S.settle_status_p2k " +     
                    //            "FROM settle_event_p2k S INNER JOIN trade_p2k T ON S.trade_no_wil=T.trade_no_wil " + filters + " "
                    //            + ") a ) b WHERE b.Settled_date >= b.filter_date ");

                    //ssql.Append("SELECT b.trade_no_wil, b.delivery_type_wil, b.settle_event_instr_p2k, b.settle_terms_wil, b.original_qty_p2k,b.open_qty_p2k, " +
                    //            "b.Settled_date,b.del_rec_ind_p2k, b.settle_status_p2k FROM(SELECT a.*, CASE WHEN a.settle_status_p2k='Y' then dateadd(mm, -2, getdate()) ELSE a.Settled_date END AS filter_date FROM " +
                    //            "(SELECT S.trade_no_wil, delivery_type_wil, settle_event_instr_p2k, settle_terms_wil, original_qty_p2k,open_qty_p2k,CASE WHEN S.settle_date_p2k <> S.version_date THEN S.version_date else S.settle_date_p2k END AS Settled_date, del_rec_ind_p2k, S.settle_status_p2k " +
                    //            "FROM settle_event_p2k S INNER JOIN trade_p2k T ON S.trade_no_wil=T.trade_no_wil " + filters + " "
                    //            + ") a ) b WHERE b.Settled_date >= b.filter_date ");

                    
                    ds = dao.ExecuteQuery(ssql.ToString());
                }

                    ds.Relations.Add("Settle_Events", ds.Tables[0].Columns[0], ds.Tables[1].Columns[0]);
                    return ds;
             }
            
            public List<Trade_Status> GetTradeListFromTE(DateTime Run_Date)
                {
                        var ds = ExtractTradesFromTE(Run_Date);
                        var dvContra = new DataView(ds.Tables["Contra_Refs"]);

                        var Trades = new Dictionary<Int64, Trade_Status>();

                        foreach (DataRow row in ds.Tables[0].Rows)
                        {
                            TradeType tradeType;
                            Enum.TryParse<TradeType>(row.Field<string>("trade_type_wil").TrimEnd(), out tradeType);

                            bool is_Contra = (tradeType == TradeType.FREC && row.Field<string>("Contra_Ref") != null);
                            if ( tradeType == TradeType.FREC || tradeType == TradeType.CAGS || is_Contra)
                            {
                                var trade = PopulateTradeInfo(Run_Date, row, is_Contra);
                                PopulateTradeEvent(row, trade);

                                Trades.Add(trade.Trade_No, trade);
                            }
                        }

                        return Trades.Values.ToList();
                   }

            private static Trade_Status PopulateTradeInfo(DateTime BusinessDate, DataRow row, bool Is_Contra)
                {
                    string Contra_Trade_Ref1 = "N";

                    var trade = new Trade_Status()
                    {
                        Business_Date = BusinessDate.Date,
                        Trade_No = Convert.ToInt64(row["trade_no_wil"].ToString().TrimEnd()),
                        Trade_Ref = row["p2000_trade_ref_p2k"].ToString().TrimEnd(),
                        Trade_Type = row["trade_type_wil"].ToString().TrimEnd(),
                    };

                    if (( row["Contra_Ref"] == null ? "" : row["Contra_Ref"].ToString().TrimEnd()) != "" && trade.Trade_Type != TradeType.FREC.ToString())
                    {
                        trade.Settle_Status = "Y";
                    }

                    return trade;
                }
                
            private static void PopulateTradeEvent(DataRow row, Trade_Status trade)
                {
                    foreach (var sRow in row.GetChildRows("Settle_Events"))
                    {
                        trade.Settle_Status = sRow["settle_status_p2k"].ToString().TrimEnd();
                        DateTime setldate = Convert.ToDateTime(sRow["Settled_date"]);
                        trade.Settle_date = setldate;
                    }
           }

            private static void PopulateSplitStatus(DataRow row, Trade_Status trade)
            {
                foreach (var linkRow in row.GetChildRows("Split_Status"))
                {
                    var link_type = linkRow["split_type_wil"].ToString().TrimEnd();
                    var split_no = linkRow["split_no_wil"].ToString().TrimEnd();

                    if (link_type == CONTRA_SPLIT_TRANSFORM)
                    {
                        trade.Settle_Status = "Y";
                    }
                }
            }

        #endregion
    }
}