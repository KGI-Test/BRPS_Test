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
    class AM_Trade_Respository : RepositoryBase
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

            private enum TradeType
            {
                FREC,
            }

            private enum OperationType
            {
                REC,
                DEL,
                CBUY,
                CSEL,
            }

            private BRPSDbContext context = new BRPSDbContext();

            #region Deconstructure
                ~AM_Trade_Respository()
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

            public int InsertTrades(List<Trade> trades, DateTime RunDate, Int32 batchNos = 100)
            {
                //string ssql = string.Format("delete Trade AS T where convert(char(8),Version_date,112)='{0:yyyyMMdd}'", DateTime.Today);
                string ssql = string.Format("DELETE FROM Trade WHERE Trade_Type = 'FREC' AND dateadd(day,0,convert(Date,Version_date, 365)) = dateadd(day,0,convert(Date, getdate(), 365))");
                context.Database.ExecuteSqlCommand(ssql);
                return InsertEntities<Trade>(context, trades);
            }

            #region extract trades from Gloss popultate trade entities

                public DataSet ExtractTradesFromTE(DateTime bizDate)
                {
                    var markets = new PartyRespository().GetAllMarkets();
                    var marketFilter = " and market_wil in ('" + string.Join("','", markets.Select(m => m.Market_Code)) + "',' ')";

                    DataSet ds = null;
                    using (var dao = new GenericDAO(new OleDbConnectionSettingsProvider()))
                    {
                        string filters = string.Format(" WHERE T.trade_type_wil IN ('{0}') AND T.trade_status_p2k IN ('A','N') {1} AND T.version_date = dateadd(day,0,convert(Date, getdate(), 365))",
                               TradeType.FREC, marketFilter);

                        var ssql = new StringBuilder("SELECT T.trade_no_wil,  p2000_trade_ref_p2k, trade_version_p2k, trade_status_p2k, entry_date_time_p2k, amend_date_wil,");
                        ssql.Append("trade_type_wil, proc_type_wil, operation_type_p2k, market_wil, trade_date_wil,  value_date_p2k,  primary_book_wil, ");
                        ssql.Append("primary_instr_p2k, primary_qty_p2k, secondary_instr_p2k,  price_decimal_p2k, price_type_p2k, mult_div_ind_p2k, factor_wil, ");
                        ssql.Append("secondary_party_p2k, settle_code_wil, settle_type_wil, settle_instr_p2k, settle_qty_p2k, trades_with_accrued_wil, accrued_interest_days_p2k, ");
                        ssql.Append("accrued_interest_rate_p2k, secondary_qty_p2k, secondary_instr_x_rate_p2k, secondary_instr_xri_p2k, accrued_interest_instr_p2k, ");
                        ssql.Append("accrued_interest_qty_p2k, accrued_interest_x_rate_p2k, accrued_interest_xri_p2k, settle_book_wil, driver_code_wil, maturity_date_p2k, ");
                        ssql.AppendFormat("Contra_Ref= (Select ext_trade_ref_wil from trade_ext_ref_wil E where T.trade_no_wil = E.record_no_wil and E.split_no_wil = 0 and E.trade_ref_type_wil='{0}') ", CONTRA_TRADE_REFERENCE);
                        ssql.Append(",proc_priority_wil, sched_no_wil, origin_wil, version_date, version_program, version_no, version_user, version_host ");
                        ssql.Append("FROM trade_p2k T");
                        ssql.Append(filters);

                        ssql.Append("SELECT C.record_no_wil, amount_type_wil, charge_levy_type_p2k,  charge_levy_instr_p2k, charge_discount_wil, leviable_instr_p2k, leviable_qty_p2k, ");
                        ssql.Append("charge_levy_qty_p2k, charge_levy_rate_p2k, rate_type_wil FROM charge_levy_p2k C INNER JOIN trade_p2k T ON C.record_no_wil=T.trade_no_wil ");
                        ssql.Append(filters);

                        ssql.Append("SELECT S.trade_no_wil, delivery_type_wil, settle_event_instr_p2k, settle_terms_wil, original_qty_p2k, open_qty_p2k, CASE WHEN S.settle_date_p2k <> S.version_date THEN S.version_date else S.settle_date_p2k END AS Settled_date, del_rec_ind_p2k, ");
                        ssql.Append("S.settle_status_p2k FROM settle_event_p2k S INNER JOIN trade_p2k T ON S.trade_no_wil=T.trade_no_wil ");
                        ssql.Append(filters);

                        ssql.Append("SELECT R.record_no_wil,record_type_wil, split_no_wil,trade_ref_type_wil, ext_trade_ref_wil, split_no_wil FROM trade_ext_ref_wil R INNER JOIN trade_p2k T ON R.record_no_wil=T.trade_no_wil ");
                        ssql.Append(filters);

                        ssql.Append("SELECT D.record_no_wil, split_no_wil,  D.driver_type_wil, D.driver_code_wil  FROM record_driver_wil D INNER JOIN trade_p2k T ON D.record_no_wil=T.trade_no_wil ");
                        ssql.Append(filters);
                        ssql.AppendFormat(" and D.driver_type_wil in ('{0}','{1}','{2}','{3}','{4}') ", TRANSACTION_SETTLEMENT_TYPE, SUB_TRANSACTION_TYPE, CASH_PAYMENT_METHOD, CASH_PAYMENT_STATUS_DRIVER, TRADE_CHANNAL_DRIVER);

                        ssql.Append("SELECT T.trade_no_wil,  p2000_trade_ref_p2k, operation_type_p2k, market_wil, primary_instr_p2k, primary_qty_p2k, ext_trade_ref_wil, split_no_wil  ");
                        ssql.Append("from trade_p2k T inner join trade_ext_ref_wil E on T.trade_no_wil=E.record_no_wil ");
                        ssql.Append(filters);
                        ssql.AppendFormat(" and E.trade_ref_type_wil='{0}' ", CONTRA_TRADE_REFERENCE);

                        ssql.Append("SELECT E.record_no_wil, E.split_no_wil, E.from_instr_id_wil,E.to_instr_id_wil, E.mult_div_ind_p2k, rate_wil ");
                        ssql.Append("FROM record_xrate_wil E INNER JOIN trade_p2k T ON E.record_no_wil=T.trade_no_wil ");
                        ssql.Append(filters);

                        ssql.Append("SELECT L.record_no_wil,record_type_wil, split_type_wil, split_no_wil, orig_split_no_wil, split_ref_wil, split_parent_wil, split_status_wil  ");
                        ssql.Append("FROM record_split_wil L INNER JOIN trade_p2k T ON L.record_no_wil=T.trade_no_wil ");
                        ssql.Append(filters);

                        ssql.Append("SELECT L.sub_record_no_wil, link_type_wil, main_type_wil, main_record_no_wil, split_no_wil, sub_type_wil,  link_qty_wil, link_status_wil  ");
                        ssql.Append("FROM link_wil L INNER JOIN trade_p2k T ON L.sub_record_no_wil=T.trade_no_wil ");
                        ssql.Append(filters);

                        //Get Cleint's EPS Bank
                        ssql.Append("SELECT T.trade_no_wil,daw.depot_alias_wil," +
                                    "(SELECT party_short_name_p2k FROM party_p2k AS P where P.party_ref_p2k = daw.depot_ref_p2k) AS EPSBankName from trade_p2k AS T ");
                        ssql.Append("INNER JOIN depot_alias_wil daw ON daw.party_ref_p2k = T.secondary_party_p2k ");
                        ssql.Append(filters);
                        ssql.AppendFormat(" and daw.depot_alias_wil IN ('{0}') ", DEPOT_ALIAS_EPS);

                        //Get Company Nostro Account and Bank Name
                        ssql.Append("SELECT T.trade_no_wil,daw.depot_alias_desc_wil AS OurNostroAccount, " +
                                    "(SELECT P.party_short_name_p2k FROM party_p2k AS P where P.party_ref_p2k = daw.depot_ref_p2k) AS OurNostroBankName from trade_p2k AS T ");
                        ssql.Append("INNER JOIN settle_event_p2k sev ON sev.trade_no_wil = T.trade_no_wil ");
                        ssql.Append("INNER JOIN depot_alias_wil daw ON daw.depot_alias_wil = sev.comp_alias_wil ");
                        ssql.Append(filters);

                        //Get Execution SalesPerson
                        ssql.Append("SELECT T.trade_no_wil,RP.trade_party_wil,RP.party_ref_p2k from record_party_wil AS RP ");
                        ssql.Append("INNER JOIN trade_p2k T ON T.trade_no_wil = RP.record_no_wil ");
                        ssql.Append(filters);
                        ssql.AppendFormat(" and RP.trade_party_wil IN ('{0}') ", EXEC_SALESPERSON);

                        ds = dao.ExecuteQuery(ssql.ToString());
                    }

                    ds.Relations.Add("Charges", ds.Tables[0].Columns[0], ds.Tables[1].Columns[0]);
                    ds.Relations.Add("Settle_Events", ds.Tables[0].Columns[0], ds.Tables[2].Columns[0]);
                    ds.Relations.Add("Ext_Refs", ds.Tables[0].Columns[0], ds.Tables[3].Columns[0]);
                    ds.Relations.Add("Record_Drivers", ds.Tables[0].Columns[0], ds.Tables[4].Columns[0]);

                    ds.Tables[5].TableName = "Contra_Refs";
                    ds.Relations.Add("Fx_Rates", ds.Tables[0].Columns[0], ds.Tables[6].Columns[0]);
                    ds.Relations.Add("Split_Status", ds.Tables[0].Columns[0], ds.Tables[7].Columns[0]);
                    ds.Relations.Add("Link_Status", ds.Tables[0].Columns[0], ds.Tables[8].Columns[0]);
                    ds.Relations.Add("Depot_Accounts", ds.Tables[0].Columns[0], ds.Tables[9].Columns[0]);
                    ds.Relations.Add("Nostro_Accounts", ds.Tables[0].Columns[0], ds.Tables[10].Columns[0]);
                    ds.Relations.Add("Execution_SalesPerson", ds.Tables[0].Columns[0], ds.Tables[11].Columns[0]);

                    return ds;
                }

                public List<Trade> GetTradeListFromTE(DateTime Run_Date)
                {
                    var ds = ExtractTradesFromTE(Run_Date);
                    var dvContra = new DataView(ds.Tables["Contra_Refs"]);

                    var Trades = new Dictionary<Int64, Trade>();

                    foreach (DataRow row in ds.Tables[0].Rows)
                    {
                        TradeType tradeType;
                        Enum.TryParse<TradeType>(row.Field<string>("trade_type_wil").TrimEnd(), out tradeType);

                        bool is_Contra = (tradeType == TradeType.FREC && row.Field<string>("Contra_Ref") != null);
                            
                        if (tradeType == TradeType.FREC || is_Contra)
                        {
                            var trade = PopulateTradeInfo(Run_Date, row, is_Contra);
                            PopulateTradeEvent(row, trade);
                            PopulateContraRef(dvContra, trade, Trades);
                            PopulateClientDepotAccounts(row, trade);
                            PopulateCompanyDepotAccounts(row, trade);
                            PopulateExecSalesPerson(row, trade);
                            PopulateTradeExtRefs(row, trade);
                            PopulateTradeDrivers(row, trade);

                            Trades.Add(trade.Trade_No, trade);
                        }
                    }

                    return Trades.Values.ToList();
                }

                private static Trade PopulateTradeInfo(DateTime BusinessDate, DataRow row, bool Is_Contra)
                {
                    var trade = new Trade()
                    {
                        Business_Date = BusinessDate.Date,
                        Trade_No = Convert.ToInt64(row["trade_no_wil"].ToString().TrimEnd()),
                        Trade_Ref = row["p2000_trade_ref_p2k"].ToString().TrimEnd(),
                        Client_Number = row["secondary_party_p2k"].ToString().TrimEnd(),
                        TR_Code = "",
                        Trade_Version = Convert.ToInt32(row["trade_version_p2k"].ToString().TrimEnd()),
                        Trade_Status = row["trade_status_p2k"].ToString().TrimEnd(),
                        Trade_Type = row["trade_type_wil"].ToString().TrimEnd(),
                        Sub_Trade_Type = row["proc_type_wil"].ToString().TrimEnd(),
                        Operation_Type = row["operation_type_p2k"].ToString().TrimEnd(),
                        Market = row["market_wil"].ToString().TrimEnd(),
                        Trade_Date = Convert.ToDateTime(row["trade_date_wil"]),
                        Value_Date = Convert.ToDateTime(row["value_date_p2k"]),
                        Primary_Book = row["primary_book_wil"].ToString().TrimEnd(),
                        Price_Type = row["price_type_p2k"].ToString().TrimEnd(),
                        Trade_Curr = row["secondary_instr_p2k"].ToString().TrimEnd(),
                        Trade_Principal = Convert.ToDecimal(row["secondary_qty_p2k"]),
                        Exch_Rate = 1M,
                        Mult_Div_Ind = "M", // row["mult_div_ind_p2k"].ToString().TrimEnd(),
                        Settle_Code = row["settle_code_wil"].ToString().TrimEnd(),
                        Settle_Type = row["settle_type_wil"].ToString().TrimEnd(),
                        Settle_Curr = row["settle_instr_p2k"].ToString().TrimEnd(),
                        Settle_Amount = Convert.ToDecimal(row["settle_qty_p2k"]),
                        Trades_With_Accrued = row["trades_with_accrued_wil"].ToString().TrimEnd(),
                        Accrued_Interest_Days = Convert.ToInt32(row["accrued_interest_days_p2k"]),
                        Accrued_Interest_Rate = Convert.ToDecimal(row["accrued_interest_rate_p2k"]),
                        Accrued_Interest_Curr = row["accrued_interest_instr_p2k"].ToString().TrimEnd(),
                        Accrued_Interest_Amt = Convert.ToDecimal(row["accrued_interest_qty_p2k"]),
                        Created_Date = Convert.ToDateTime(row["entry_date_time_p2k"]),
                        Amend_Date = Convert.ToDateTime(row["amend_date_wil"]),
                        Version_date = Convert.ToDateTime(row["version_date"]),
                        Version_no = Convert.ToInt32(row["version_no"]),
                        Contra_Trade_Ref = row["Contra_Ref"] == null ? "" : row["Contra_Ref"].ToString().TrimEnd(),
                        Settle_Transform = "",
                    };

                    if (trade.Operation_Type == "CBUY" || trade.Operation_Type == "BUY")
                    {
                        trade.Settle_Amount = -trade.Settle_Amount;
                    }

                    if (trade.Contra_Trade_Ref != "" && trade.Trade_Type != TradeType.FREC.ToString())
                    {
                        trade.Settle_Transform = CONTRA_TRANSFORM;
                        trade.Settle_Status = "Y";
                    }

                    PopulateSplitStatus(row, trade);
                    PopulateLinkStatus(row, trade);

                    string Mult_Div_Ind;
                    trade.Exch_Rate = GetSGDExchangeRate(row, trade.Trade_Curr, out Mult_Div_Ind);
                    trade.Mult_Div_Ind = Mult_Div_Ind;

                    SetAmountByTradeStatus(row, trade);

                    return trade;
                }

                private static void PopulateExecSalesPerson(DataRow row, Trade trade)
                {
                    foreach (var acctRow in row.GetChildRows("Execution_SalesPerson"))
                    {
                        var partytype = acctRow["trade_party_wil"].ToString().TrimEnd();
                        var execsp = acctRow["party_ref_p2k"].ToString().TrimEnd();

                        switch (partytype)
                        {
                            case EXEC_SALESPERSON:
                                trade.Execution_SalesPerson = execsp;
                                break;
                            default:
                                break;
                        }
                    }
                }

                private static void PopulateClientDepotAccounts(DataRow row, Trade trade)
                {
                    foreach (var acctRow in row.GetChildRows("Depot_Accounts"))
                    {
                        var depotAlias = acctRow["depot_alias_wil"].ToString().TrimEnd();
                        var eps_bankname = acctRow["EPSBankName"].ToString().TrimEnd();

                        switch (depotAlias)
                        {
                            case DEPOT_ALIAS_EPS:
                                trade.EPS_Bank_Name = eps_bankname;
                                break;
                            default:
                                break;
                        }
                    }
                }

                private static void PopulateCompanyDepotAccounts(DataRow row, Trade trade)
                {
                    foreach (var acctRow in row.GetChildRows("Nostro_Accounts"))
                    {
                        var OurNostroAccount = acctRow["OurNostroAccount"].ToString().TrimEnd();
                        var OurNostroBankName = acctRow["OurNostroBankName"].ToString().TrimEnd();

                        trade.Our_Nostro_Account = OurNostroAccount;
                        trade.Our_Nostro_BankName = OurNostroBankName;
                    }
                }

                private static void PopulateTradeExtRefs(DataRow row, Trade trade)
                {
                    foreach (var eRow in row.GetChildRows("Ext_Refs"))
                    {
                        var ext_trade = eRow.Field<string>("ext_trade_ref_wil").TrimEnd();
                        var trade_type_ref = eRow.Field<string>("trade_ref_type_wil").TrimEnd();

                        switch (trade_type_ref)
                        {
                            case FRONT_OFFICE_REFERENCE_UNIQUE:
                                trade.FOR_No = ext_trade;
                                break;
                            case ALLOCATION_PROCESSING_INDICATOR:
                                trade.Allocation_Processing_Indicator = ext_trade;
                                break;
                            default:
                                break;
                        }
                    }
                }

                private static void PopulateTradeDrivers(DataRow row, Trade trade)
                {
                    foreach (var eRow in row.GetChildRows("Record_Drivers"))
                    {
                        var data = eRow.Field<string>("driver_code_wil");
                        switch (eRow.Field<string>("driver_type_wil").TrimEnd())
                        {
                            case TRANSACTION_SETTLEMENT_TYPE:
                                trade.Trans_Settle_Type = data;
                                break;
                            case SUB_TRANSACTION_TYPE:
                                trade.Sub_Trade_Type = data;
                                break;
                            case CASH_PAYMENT_METHOD:
                                trade.Cash_Payment_Mode = data;
                                break;
                            case CASH_PAYMENT_STATUS_DRIVER:
                                trade.Cash_Payment_Status = data;
                                break;
                            case TRADE_CHANNAL_DRIVER:
                                trade.Trade_Channel = data;
                                break;
                            default:
                                break;
                        }
                    }
                }

                private static void PopulateTradeEvent(DataRow row, Trade trade)
                {
                    foreach (var sRow in row.GetChildRows("Settle_Events"))
                    {
                        var del_rec_ind = sRow["del_rec_ind_p2k"].ToString().TrimEnd();
                        trade.Settle_Status = sRow["settle_status_p2k"].ToString().TrimEnd();
                        var deliveryType = sRow["delivery_type_wil"].ToString().TrimEnd();
                        var settleAmt = Convert.ToDecimal(sRow["original_qty_p2k"]);
                        var openAmt = Convert.ToDecimal(sRow["open_qty_p2k"]);
                        DateTime setldate = Convert.ToDateTime(sRow["Settled_date"]);
                        trade.Settle_date = setldate;

                        switch (deliveryType)
                        {
                            case PRIMARY_SETTLEMENT:
                            case PRIMARY_SIDE_2:
                                trade.Qty = Convert.ToInt32(settleAmt);
                                trade.Open_Qty = Convert.ToInt32(openAmt);
                                break;
                            case SECONDARY_SETTLEMENT:
                                trade.Settle_Amount = settleAmt;
                                trade.Open_Amount = openAmt;
                                if ((del_rec_ind == "R" && trade.Trade_Type != TradeType.FREC.ToString()) ||
                                    (del_rec_ind == "D" && trade.Trade_Type == TradeType.FREC.ToString()))
                                {
                                    trade.Open_Amount = -trade.Open_Amount;
                                    trade.Settle_Amount = -trade.Settle_Amount;
                                }
                                trade.Settle_Amount_SGD = ConvertAmountToSGD(trade.Open_Amount, trade.Exch_Rate, trade.Mult_Div_Ind);
                                break;
                            case SECONDARY_SIDE_2:
                                trade.Settle_Amount = settleAmt;
                                trade.Open_Amount = openAmt;
                                if (del_rec_ind == "D")
                                {
                                    trade.Open_Amount = -trade.Open_Amount;
                                    trade.Settle_Amount = -trade.Settle_Amount;
                                }
                                trade.Settle_Amount_SGD = ConvertAmountToSGD(trade.Open_Amount, trade.Exch_Rate, trade.Mult_Div_Ind);
                                break;
                            default:
                                break;
                        }
                    }
                }

                private static void PopulateTradeCharges(DataRow row, Trade trade)
                {
                    var charges = new TradeCharge();
                    trade.TradeCharge = charges;
                    charges.Business_Date = trade.Business_Date;
                    charges.Trade_No = trade.Trade_No;
                    charges.Curr = trade.Trade_Curr;

                    foreach (var cRow in row.GetChildRows("Charges"))
                    {
                        var amt = Convert.ToDecimal(cRow["charge_levy_qty_p2k"]);
                        var curr = cRow["charge_levy_instr_p2k"].ToString().TrimEnd();
                        switch (cRow["charge_levy_type_p2k"].ToString().TrimEnd())
                        {
                            case COMMISSION:
                                charges.Commission = amt;
                                charges.Commission_SGD = ConvertAmountToSGD(amt, trade.Exch_Rate, trade.Mult_Div_Ind);
                                break;
                            case SGX_CLEARING_FEE:
                                charges.Clearing_Fee = amt;
                                charges.Clearing_Fee_SGD = ConvertAmountToSGD(amt, trade.Exch_Rate, trade.Mult_Div_Ind);
                                break;
                            case SINGAPORE_ACCESS_FEE:
                            case HONG_KONG_TRADING_FEE:
                                charges.Trading_Fee = amt;
                                charges.Trading_Fee_SGD = ConvertAmountToSGD(amt, trade.Exch_Rate, trade.Mult_Div_Ind);
                                break;
                            case HONG_KONG_STAMP_DUTY:
                                charges.Stamp_Duty_Fee = amt;
                                charges.Stamp_Duty_Fee_SGD = ConvertAmountToSGD(amt, trade.Exch_Rate, trade.Mult_Div_Ind);
                                break;
                            case HONG_KONG_TRANSACTION_LEVY:
                                charges.Transaction_Levy_Fee = amt;
                                charges.Trans_Levy_Fee_SGD = ConvertAmountToSGD(amt, trade.Exch_Rate, trade.Mult_Div_Ind);
                                break;
                            case SETTLEMENT_FEE:
                                charges.Settlement_Fee = amt;
                                charges.Settlement_Fee_SGD = ConvertAmountToSGD(amt, trade.Exch_Rate, trade.Mult_Div_Ind);
                                break;
                            case SG_GST_ON_COMMISSION_SGD:
                                charges.Commission_GST_SGD = amt;
                                break;
                            case SG_GST_ON_CLEARING_FEE_SGD:
                                charges.Clearing_Fee_GST_SGD = amt;
                                break;
                            case SG_GST_ON_TRADING_FEE_SGD:
                                charges.Trading_Fee_GST_SGD = amt;
                                break;
                            case SG_GST_ON_COMMISSION:
                                charges.Commission_GST = amt;
                                break;
                            case SG_GST_ON_CLEARING_FEE:
                                charges.Clearing_Fee_GST = amt;
                                break;
                            case SG_GST_ON_TRADING_FEE:
                                charges.Trading_Fee_GST = amt;
                                break;
                        }
                    }
                }
                
                private static void PopulateSplitStatus(DataRow row, Trade trade)
                {
                    foreach (var linkRow in row.GetChildRows("Split_Status"))
                    {
                        var link_type = linkRow["split_type_wil"].ToString().TrimEnd();
                        var split_no = linkRow["split_no_wil"].ToString().TrimEnd();

                        if (link_type == CONTRA_SPLIT_TRANSFORM)
                        {
                            trade.Settle_Transform = CONTRA_SPLIT_TRANSFORM;
                            trade.Settle_Status = "Y";

                            if (Convert.ToInt64(split_no.ToString()) > 0)
                            {
                                trade.Parent_Link_Trade = "Y";
                            }
                            else
                            {
                                trade.Parent_Link_Trade = "N";
                            }
                        }
                        else
                        {
                            trade.Parent_Link_Trade = "N";
                        }
                    }
                }

                private static void PopulateLinkStatus(DataRow row, Trade trade)
                {
                    foreach (var linkRow in row.GetChildRows("Link_Status"))
                    {
                        var link_type = linkRow["link_type_wil"].ToString().TrimEnd();
                        trade.Trade_No_Ref1 = Convert.ToInt64(string.IsNullOrEmpty(linkRow["main_record_no_wil"].ToString()) ? 0 : Convert.ToInt64(linkRow["main_record_no_wil"]));
                        trade.Trade_No_Ref2 = Convert.ToInt64(string.IsNullOrEmpty(linkRow["split_no_wil"].ToString()) ? 0 : Convert.ToInt64(linkRow["split_no_wil"]));
                    }
                }

                private static void SetAmountByTradeStatus(DataRow row, Trade trade)
                {
                    if (trade.Trade_Type != TradeType.FREC.ToString())
                    {
                        trade.Instrument = row["primary_instr_p2k"].ToString().TrimEnd();
                        trade.Qty = Convert.ToInt32(row["primary_qty_p2k"]);
                        trade.Price = Convert.ToDecimal(row["price_decimal_p2k"]);
                        if (trade.Settle_Status != "Y")
                        {
                            trade.Open_Qty = trade.Qty;
                        }
                    }
                    else
                    {
                        trade.Settle_Curr = row["primary_instr_p2k"].ToString().TrimEnd();
                        trade.Settle_Amount = Convert.ToDecimal(row["primary_qty_p2k"]);
                        if (trade.Operation_Type == OperationType.DEL.ToString())
                        {
                            trade.Settle_Amount = -trade.Settle_Amount;
                        }
                        trade.Open_Qty = 0;
                    }

                    if (trade.Settle_Status != "Y")
                    {
                        trade.Settle_Amount_SGD = ConvertAmountToSGD(trade.Settle_Amount, trade.Exch_Rate, trade.Mult_Div_Ind);
                        trade.Open_Amount = trade.Settle_Amount;
                    }
                }

                private static void PopulateContraRef(DataView dvContra, Trade trade, Dictionary<Int64, Trade> trades)
                {
                    dvContra.RowFilter = String.Format("ext_trade_ref_wil='{0}'", trade.Contra_Trade_Ref);

                    Int32 buyQty = 0, sellQty = 0;
                    trade.Open_Qty = 0;
                    foreach (DataRowView rowView in dvContra)
                    {
                        var row = rowView.Row;
                        var split_no = Convert.ToInt32(row["split_no_wil"]);

                        var operation_type = row["operation_type_p2k"].ToString().TrimEnd();
                        var trade_no = Convert.ToInt32(row["trade_no_wil"]);

                        if (!trades.ContainsKey(trade_no))
                        {
                            continue;
                        }

                        var relatedTrade = trades[trade_no];
                        if (relatedTrade.Settle_Transform == CONTRA_SPLIT_TRANSFORM)
                        {
                            continue;
                        }

                        OperationType operationType;
                        Enum.TryParse<OperationType>(operation_type, out operationType);
                        if (operationType == OperationType.CBUY)
                        {
                            trade.Trade_No_Ref1 = trade_no;
                            trade.Market = row["market_wil"].ToString().TrimEnd();
                            trade.Instrument = row["primary_instr_p2k"].ToString().TrimEnd();
                            buyQty = Convert.ToInt32(row["primary_qty_p2k"]);
                        }
                        else if (operationType == OperationType.CSEL)
                        {
                            trade.Trade_No_Ref2 = trade_no;
                            trade.Market = row["market_wil"].ToString().TrimEnd();
                            trade.Instrument = row["primary_instr_p2k"].ToString().TrimEnd();
                            sellQty = Convert.ToInt32(row["primary_qty_p2k"]);
                        }
                    }
                    trade.Qty = Math.Min(buyQty, sellQty);
                }

                private static decimal ConvertAmountToSGD(decimal origAmount, decimal fxRate, string mult_Div_Ind, int roundingDecimal = 2)
                {
                    if (fxRate == 0M)
                    {
                        fxRate = 1M;
                    }

                    if (mult_Div_Ind == "M")
                    {
                        return Math.Round(origAmount * fxRate, roundingDecimal);
                    }
                    else
                    {
                        return Math.Round(origAmount / fxRate, roundingDecimal);
                    }
                }

                private static decimal GetSGDExchangeRate(DataRow row, string settle_Curr, out string Multi_Div_Ind)
                {
                    decimal Exch_Rate = 1M;
                    Multi_Div_Ind = "M";
                    foreach (var fxRow in row.GetChildRows("Fx_Rates"))
                    {
                        var fromCcy = fxRow["from_instr_id_wil"].ToString().TrimEnd();
                        var toCcy = fxRow["to_instr_id_wil"].ToString().TrimEnd();

                        if (fromCcy == settle_Curr && toCcy == "SGD" && fromCcy != "SGD")
                        {
                            Exch_Rate = Convert.ToDecimal(fxRow["rate_wil"]); ;
                            Multi_Div_Ind = fxRow["mult_div_ind_p2k"].ToString().TrimEnd();
                        }
                    }
                    return Exch_Rate == 0 ? 1M : Exch_Rate;
                }
            
            #endregion       
    }
}
