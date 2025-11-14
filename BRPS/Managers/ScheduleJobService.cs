using BRPS.Model;
using BRPS.Model.Extensions;
using BRPS.Repositories;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;

namespace BRPS.Managers
{
    public interface IScheduledJobService
    {
        void Execute(ScheduledJobEx jobModel, DateTime Business_Date, bool AutoMode, KGILogger objLogger);
    }

    class ScheduledJobService : IScheduledJobService
    {
        private readonly string RECORD_SAVED_MESSAGE = "The total {0} record(s) has(ve) loaded into system.";
        private readonly string NO_RECORD_SAVED_MESSAGE = "No record has been saved.";
        private readonly string METHOD_NOT_EXIST = "The method {0} of {1} does not exist.";
        private readonly string EXCEPTION_MESSAGE = "Error happened in {0}. Cause: {1}";
        private readonly string INVAID_ARGUREMENT_NULL = "The argurement {0} cannot be null";
        private readonly string INVAID_APP_SETTING_KEY = "The appSetting {0} is not valid";
        public string Message { get; set; }

        KGILogger m_logger;

        public void initLog(KGILogger objlogger)
        {
            try
            {
                if (this.m_logger == null)
                {
                    m_logger = objlogger;
                }

                m_logger.TurnOnDebugScreen = false;                
            }
            catch (Exception Ex)
            {
                if (this.m_logger != null)
                {
                    this.m_logger.Error("initLog - Error: " + Ex.Message);
                }
            }
        }

        public void Execute(ScheduledJobEx jobModel, DateTime Business_Date, bool AutoMode, KGILogger objLogger)
        {
            try
            {
                this.initLog(objLogger);
                var job = jobModel.Job;

                //The job has been executed successfully.
                var isDone = jobModel.Business_Date.Equals(Business_Date) && (jobModel.Status == 1);

                var curTime = DateTime.Now.ToString("HH:mm");
                var inRunTime = ((job.End_Time.CompareTo(job.From_Time) > 0 && curTime.CompareTo(job.From_Time) >= 0 && curTime.CompareTo(job.End_Time) < 0) ||
                        (job.End_Time.CompareTo(job.From_Time) < 0 && (curTime.CompareTo(job.From_Time) >= 0 || curTime.CompareTo(job.End_Time) < 0)));

                if (AutoMode && (isDone || !inRunTime))
                {
                    return;
                }

                if (jobModel.Selected && !AutoMode && !inRunTime )
                {
                    if(MessageBox.Show(String.Format("The current time is not between scheduled job from {0} to {1}.\nAre sure to continue?", job.From_Time, job.End_Time),
                            "Confirm Execution", MessageBoxButton.YesNo,MessageBoxImage.Question)==MessageBoxResult.No)
                    {
                        return;
                    }
                }               
                
                var parameters = new List<object>();
                //jobModel.Business_Date = Business_Date;
                jobModel.Business_Date = DateTime.Now.AddDays(-1);
                jobModel.Last_Execution_Time = DateTime.Now;

                foreach (ScheduledJobParam parameter in job.ScheduledJobParams.OrderBy(para => para.Param_Order))
                {
                    jobModel.Message = string.Format("Under For Each Loop");

                    if (parameter.Param_Name == "DATE")
                    {
                        parameters.Add(DateTime.Now.Date.AddDays(Convert.ToInt32(parameter.Param_Value)));
                    }
                    else if (parameter.Param_Name == "BUSINESS_DATE")
                    {
                        parameters.Add(Business_Date);
                    }
                    else if (parameter.Param_Name == "APP_SETTING")
                    {
                        var val = ConfigurationManager.AppSettings[parameter.Param_Value].ToString();
                        if(val==null)
                        {
                            jobModel.Message = string.Format(INVAID_APP_SETTING_KEY, parameter.Param_Value);
                            jobModel.Status = 2;
                            return;
                        }

                        Type type = null;
                        if (!String.IsNullOrEmpty(parameter.Param_Type))
                        {
                            type = Type.GetType(parameter.Param_Type);
                        }
                        parameters.Add(type != null ? Convert.ChangeType(val, type) : val);
                    }
                    else
                    {
                        Type type = null;
                        if(!String.IsNullOrEmpty(parameter.Param_Type))
                        {
                            type=Type.GetType(parameter.Param_Type);
                        }
                        parameters.Add(type != null ? Convert.ChangeType(parameter.Param_Value, type) : parameter.Param_Value);
                    }
                }

                var mi = this.GetType().GetMethod(job.Process,Type.GetTypeArray( parameters.ToArray()));

                jobModel.Message = string.Format("After the function");
                if (mi == null)
                {
                    jobModel.Message =string.Format(METHOD_NOT_EXIST,  job.Process , this.GetType().ToString()) ;
                    jobModel.Status = 2;
                    return;
                }
                var nos = (int)mi.Invoke(this, parameters.ToArray());
                jobModel.Message = string.Format("no = " ,nos.ToString());
                if (nos < 0)
                {
                    jobModel.Message = Message;
                    jobModel.Status = 2;
                }
                else
                {
                    jobModel.Message = (nos == 0)? NO_RECORD_SAVED_MESSAGE : string.Format(RECORD_SAVED_MESSAGE, nos);
                    jobModel.Status = 1;
                }
            }
            catch (Exception ex)
            {
                jobModel.Message = string.Format(EXCEPTION_MESSAGE, "Execute" , (ex.InnerException ?? ex).Message);
                jobModel.Status = 2;
            }
        }
    
        public int LoadInstruments()
        {
            try
            {
                m_logger.Info("+LoadInstruments()");

                Message = "";

                var repository = new InstrumentRepository();

                m_logger.Info("LoadInstruments():+GetInstrumentListFromTE");
                var instrList = repository.GetInstrumentListFromTE();
                m_logger.Info("LoadInstruments():-GetInstrumentListFromTE");

                m_logger.Info("LoadInstruments():+InsertInstruments");
                repository.InsertInstruments(instrList);
                m_logger.Info("LoadInstruments():-InsertInstruments");

                m_logger.Info("-LoadInstruments()");
                return instrList.Count;
            }
            catch(Exception ex)
            {
                Console.WriteLine(ex.Message);
                Message = string.Format(EXCEPTION_MESSAGE, "Load Instruments", (ex.InnerException ?? ex).Message);
                m_logger.Error(Message);
            }

            m_logger.Info("-LoadInstruments()");
            return -1;
        }

        public int LoadInstruments_Price()
        {
            try
            {
                m_logger.Info("+LoadInstruments_Price()");

                var curTime = DateTime.Now.ToString("HH:mm");
                    Message = "";

                //Get Instrument Price , FX
                var instrpxrep1 = new InstrumentPriceRepository();
                var instrPxList1 = instrpxrep1.GetInstPriceFXFromTE();
                instrpxrep1.InsertInstrumentFXPxInfo(instrPxList1);
                    
                //Get Instrument Price, LDP
                var instrpxrep = new InstrumentPriceRepository();

                m_logger.Info("LoadInstruments_Price():+GetInstPriceFromTE");
                var instrPxList = instrpxrep.GetInstPriceFromTE();
                m_logger.Info("LoadInstruments_Price():-GetInstPriceFromTE");

                m_logger.Info("LoadInstruments_Price():+InsertInstrumentPxInfo");
                instrpxrep.InsertInstrumentPxInfo(instrPxList);
                m_logger.Info("LoadInstruments_Price():-InsertInstrumentPxInfo");

                m_logger.Info("LoadInstruments_Price():+Update_Instrument_ClosePrice");
                instrpxrep.Update_Instrument_ClosePrice();
                m_logger.Info("LoadInstruments_Price():-Update_Instrument_ClosePrice");

                m_logger.Info("-LoadInstruments_Price()");
                return (instrPxList.Count + instrPxList1.Count);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                Message = string.Format(EXCEPTION_MESSAGE, "Load Instruments Price", (ex.InnerException ?? ex).Message);
                m_logger.Error(Message);
            }

            m_logger.Info("-LoadInstruments_Price()");
            return -1;
        }

        public int LoadSalespersons()
        {
            try
            {
                m_logger.Info("+LoadSalespersons()");

                Message = "";
                var repository = new PartyRespository();

                m_logger.Info("LoadSalespersons():+GetPartyListFromGloss");
                var parties = repository.GetPartyListFromGloss(PartyCategory.SALE, new DateTime(1900, 01, 01));
                m_logger.Info("LoadSalespersons():-GetPartyListFromGloss");

                m_logger.Info("LoadSalespersons():+ReloadAllParties");
                repository.ReloadAllParties(PartyCategory.SALE, parties);
                m_logger.Info("LoadSalespersons():-ReloadAllParties");

                m_logger.Info("-LoadSalespersons()");

                return parties.Count;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                Message = string.Format(EXCEPTION_MESSAGE, "Load Salepersons", (ex.InnerException ?? ex).Message);
                m_logger.Error(Message);
            }

            m_logger.Info("-LoadSalespersons()");

            return -1;
        }

        public int LoadClients(DateTime lastBusinessDate)
        {
            try
            {
                m_logger.Info("+LoadClients()");
                Message = "";
                var repository = new PartyRespository();

                m_logger.Info("LoadClients():+GetPartyListFromGloss");
                var parties = repository.GetPartyListFromGloss(PartyCategory.SECP, new DateTime(1900, 01, 01));
                m_logger.Info("LoadClients():-GetPartyListFromGloss");

                m_logger.Info("LoadClients():+ReloadAllParties");
                repository.ReloadAllParties(PartyCategory.SECP, parties);
                m_logger.Info("LoadClients():-ReloadAllParties");


                m_logger.Info("-LoadClients()");
                return parties.Count;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                Message = string.Format(EXCEPTION_MESSAGE, "Load Clients", (ex.InnerException ?? ex).Message);
                m_logger.Error(Message);
            }

            m_logger.Info("-LoadClients()");
            return -1;
        }

        public int LoadClients_EOD(DateTime lastBusinessDate)
        {
            try
            {
                m_logger.Info("+LoadClients_EOD()");
                Message = "";
                
                var repository = new PartyRespository();
                
                m_logger.Info("LoadClients_EOD():+GetPartyListFromGloss");
                var parties = repository.GetPartyListFromGloss(PartyCategory.SECP, new DateTime(1900, 01, 01));
                m_logger.Info("LoadClients_EOD():-GetPartyListFromGloss");

                m_logger.Info("LoadClients_EOD():+ReloadAllParties");
                repository.ReloadAllParties(PartyCategory.SECP, parties);
                m_logger.Info("LoadClients_EOD():-ReloadAllParties");

                m_logger.Info("-LoadClients_EOD()");
                return parties.Count;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                Message = string.Format(EXCEPTION_MESSAGE, "Load Clients - EOD - 6PM", (ex.InnerException ?? ex).Message);
                m_logger.Error(Message);
            }

            m_logger.Info("-LoadClients_EOD()");
            return -1;
        }

        public int LoadStockBalance()
        {            
            try
            {
                m_logger.Info("+LoadStockBalance()");

                Message = "";                

                //Get Client Stock Balance
                var clistkbalRepo = new ClientBalanceRepository();
                
                m_logger.Info("LoadStockBalance():+GetClientStockBalanceFromTE");
                var clibal = clistkbalRepo.GetClientStockBalanceFromTE();
                m_logger.Info("LoadStockBalance():-GetClientStockBalanceFromTE");

                m_logger.Info("LoadStockBalance():+InsertClientStockBalanceInfo");
                clistkbalRepo.InsertClientStockBalanceInfo(clibal);
                m_logger.Info("LoadStockBalance():-InsertClientStockBalanceInfo");

                m_logger.Info("-LoadStockBalance()");

                return clibal.Count;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                Message = string.Format(EXCEPTION_MESSAGE, "Load Stock Balance", (ex.InnerException ?? ex).Message);
                m_logger.Error(Message);
            }

            m_logger.Info("-LoadStockBalance()");

            return -1;
        }

        public int LoadCashBalance()
        {
            try
            {
                Message = "";

                //Get Client Cash Balance
                var clicshbalRepo = new ClientBalanceRepository();
                var clibal = clicshbalRepo.GetClientCashBalanceFromTE();
                clicshbalRepo.InsertClientCashBalanceInfo(clibal);

                return clibal.Count;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                Message = string.Format(EXCEPTION_MESSAGE, "Load Cash Balance", (ex.InnerException ?? ex).Message);
            }
            return -1;
        }

        public int LoadTradeNarrativeInfo()
        {
            try
            {
                Message = "";

                //Get Trade Narrative
                var narrinforep = new TradeNarrativeRespository();
                var narrinfo = narrinforep.GetTradeNarrativeFromTE();
                narrinforep.InsertTradeNarrativeInfo(narrinfo);

                return narrinfo.Count;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                Message = string.Format(EXCEPTION_MESSAGE, "Load Trade Narrative", (ex.InnerException ?? ex).Message);
            }
            return -1;
        }

        public int LoadAmendedTrade()
        {
            try
            {
                Message = "";

                //Get Amended Trades
                var AmdTrdCom = new TradeCharge_AmendedComm_Respository();
                var amdlst = AmdTrdCom.GetTradeCharge_AmendedComm_FromTE();
                AmdTrdCom.InsertExtractTradeCharge_AmendedComm_FromTE(amdlst);

                return amdlst.Count;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                Message = string.Format(EXCEPTION_MESSAGE, "Load Amended Trade", (ex.InnerException ?? ex).Message);
            }
            return -1;
        }

        public int LoadCancelledTrade(DateTime lastBusinessDate)
        {
            try
            {
                Message = "";
                if (lastBusinessDate == null)
                {
                    Message = string.Format(INVAID_ARGUREMENT_NULL, "lastBusinessDate");
                    return -1;
                }

                ////Get Cancelled Trades
                var repository1 = new TradeCancelledRepository();
                var tradeList1 = repository1.GetTradeListFromTE(lastBusinessDate);
                repository1.InsertTrades(tradeList1);

                return tradeList1.Count;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                Message = string.Format(EXCEPTION_MESSAGE, "Load Cancelled Trade", (ex.InnerException ?? ex).Message);
            }
            return -1;
        }

        public int LoadTrades_BOD(DateTime lastBusinessDate)
        {
            try
            {
                var curTime = DateTime.Now.ToString("HH:mm");

                //Timming
                if (curTime.CompareTo("08:45") > 0 && curTime.CompareTo("23:30") < 0)
                {
                    Message = "";
                    if (lastBusinessDate == null)
                    {
                        Message = string.Format(INVAID_ARGUREMENT_NULL, "lastBusinessDate");
                        return -1;
                    }

                    //Get Latest Trade Status
                    var AM_Trade_Respository = new Trade_Status_Respository();
                    var tradeList1 = AM_Trade_Respository.GetTradeListFromTE(lastBusinessDate);
                    AM_Trade_Respository.InsertTrades(tradeList1, DateTime.Today);

                    //Get Trade(FREC,EPSG,REC) - SGX API
                    var AM_Trade_EPS_Respository = new AM_Trade_Respository();
                    var tradeList2 = AM_Trade_EPS_Respository.GetTradeListFromTE(lastBusinessDate);
                    AM_Trade_EPS_Respository.InsertTrades(tradeList2, lastBusinessDate);

                    return tradeList2.Count;
                }
                else
                {
                    return -1;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                Message = string.Format(EXCEPTION_MESSAGE, "Load Trades - BEGIN OF DAY", (ex.InnerException ?? ex).Message);
            }
            return -1;
        }

        public int LoadTrades_EOD_CAGS(DateTime lastBusinessDate)
        {
            

            try
            {
                m_logger.Info("+LoadTrades_EOD_CAGS()");

                Message = "";
                if (lastBusinessDate == null)
                {
                    Message = string.Format(INVAID_ARGUREMENT_NULL, "lastBusinessDate");
                    return -1;
                }

                var repository2 = new TradeRepository();

                m_logger.Info("LoadTrades_EOD_CAGS():+GetTradeListFromTE_CAGS");
                var tradeList2 = repository2.GetTradeListFromTE_CAGS(lastBusinessDate);
                m_logger.Info("LoadTrades_EOD_CAGS():-GetTradeListFromTE_CAGS");

                m_logger.Info("LoadTrades_EOD_CAGS():+InsertTrades_CAGS");
                repository2.InsertTrades_CAGS(tradeList2, lastBusinessDate);
                m_logger.Info("LoadTrades_EOD_CAGS():-InsertTrades_CAGS");


                m_logger.Info("-LoadTrades_EOD_CAGS()");
                return tradeList2.Count;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                Message = string.Format(EXCEPTION_MESSAGE, "Load Trades - EOD - CAGS", (ex.InnerException ?? ex).Message);
            }

            m_logger.Info("-LoadTrades_EOD_CAGS()");
            return -1;
        }

        public int LoadTrades_EOD_BAGS(DateTime lastBusinessDate)
        {
            try
            {
                m_logger.Info("+LoadTrades_EOD_BAGS()");

                Message = "";
                if (lastBusinessDate == null)
                {
                    Message = string.Format(INVAID_ARGUREMENT_NULL, "lastBusinessDate");
                    return -1;
                }

                var repository2 = new TradeRepository();

                m_logger.Info("LoadTrades_EOD_BAGS():+GetTradeListFromTE_BAGS");
                var tradeList2 = repository2.GetTradeListFromTE_BAGS(lastBusinessDate);
                m_logger.Info("LoadTrades_EOD_BAGS():-GetTradeListFromTE_BAGS");

                m_logger.Info("LoadTrades_EOD_BAGS():+InsertTrades_BAGS");
                repository2.InsertTrades_BAGS(tradeList2, lastBusinessDate);
                m_logger.Info("LoadTrades_EOD_BAGS():-InsertTrades_BAGS");

                m_logger.Info("-LoadTrades_EOD_BAGS()");

                return tradeList2.Count;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                Message = string.Format(EXCEPTION_MESSAGE, "Load Trades - EOD - BAGS", (ex.InnerException ?? ex).Message);
                m_logger.Error(Message);
            }

            m_logger.Info("-LoadTrades_EOD_BAGS()");

            return -1;
        }

        public int LoadTrades_EOD_CCAC(DateTime lastBusinessDate)
        {
            try
            {
                m_logger.Info("+LoadTrades_EOD_CCAC()");

                Message = "";
                if (lastBusinessDate == null)
                {
                    Message = string.Format(INVAID_ARGUREMENT_NULL, "lastBusinessDate");
                    return -1;
                }

                var repository2 = new TradeRepository();

                m_logger.Info("LoadTrades_EOD_CCAC():+GetTradeListFromTE_CCAC");
                var tradeList2 = repository2.GetTradeListFromTE_CCAC(lastBusinessDate);
                m_logger.Info("LoadTrades_EOD_CCAC():-GetTradeListFromTE_CCAC");

                m_logger.Info("LoadTrades_EOD_CCAC():+InsertTrades_CCAC");
                repository2.InsertTrades_CCAC(tradeList2, lastBusinessDate);
                m_logger.Info("LoadTrades_EOD_CCAC():-InsertTrades_CCAC");
                
                m_logger.Info("-LoadTrades_EOD_CCAC()");

                return tradeList2.Count;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                Message = string.Format(EXCEPTION_MESSAGE, "Load Trades - EOD - CCAC", (ex.InnerException ?? ex).Message);
                m_logger.Error(Message);
            }

            m_logger.Info("-LoadTrades_EOD_CCAC()");

            return -1;
        }

        public int LoadTrades_EOD_SCCF(DateTime lastBusinessDate)
        {
            try
            {
                m_logger.Info("+LoadTrades_EOD_SCCF()");

                Message = "";
                if (lastBusinessDate == null)
                {
                    Message = string.Format(INVAID_ARGUREMENT_NULL, "lastBusinessDate");
                    return -1;
                }

                var repository2 = new TradeRepository();

                m_logger.Info("LoadTrades_EOD_SCCF():+GetTradeListFromTE_SCCF");
                var tradeList2 = repository2.GetTradeListFromTE_SCCF(lastBusinessDate);
                m_logger.Info("LoadTrades_EOD_SCCF():-GetTradeListFromTE_SCCF");

                m_logger.Info("LoadTrades_EOD_SCCF():+InsertTrades_SCCF");
                repository2.InsertTrades_SCCF(tradeList2, lastBusinessDate);
                m_logger.Info("LoadTrades_EOD_SCCF():-InsertTrades_SCCF");
                                
                m_logger.Info("-LoadTrades_EOD_SCCF()");

                return tradeList2.Count;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                Message = string.Format(EXCEPTION_MESSAGE, "Load Trades - EOD - SCCF", (ex.InnerException ?? ex).Message);
                m_logger.Error(Message);
            }

            m_logger.Info("-LoadTrades_EOD_SCCF()");

            return -1;
        }

        public int LoadTrades_EOD_FRES(DateTime lastBusinessDate)
        {
            try
            {
                m_logger.Info("+LoadTrades_EOD_FRES()");
                Message = "";
                if (lastBusinessDate == null)
                {
                    Message = string.Format(INVAID_ARGUREMENT_NULL, "lastBusinessDate");
                    return -1;
                }

                var repository2 = new TradeRepository();

                m_logger.Info("LoadTrades_EOD_FRES():+GetTradeListFromTE_FRES");
                var tradeList2 = repository2.GetTradeListFromTE_FRES(lastBusinessDate);
                m_logger.Info("LoadTrades_EOD_FRES():-GetTradeListFromTE_FRES");

                m_logger.Info("LoadTrades_EOD_FRES():+InsertTrades_FRES");
                repository2.InsertTrades_FRES(tradeList2, lastBusinessDate);
                m_logger.Info("LoadTrades_EOD_FRES():-InsertTrades_FRES");



                m_logger.Info("-LoadTrades_EOD_FRES()");

                return tradeList2.Count;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                Message = string.Format(EXCEPTION_MESSAGE, "Load Trades - EOD - FRES", (ex.InnerException ?? ex).Message);
                m_logger.Error(Message);
            }

            m_logger.Info("-LoadTrades_EOD_FRES()");
            return -1;
        }

        public int LoadTrades_EOD_FREC_OPEN(DateTime lastBusinessDate)
        {
            try
            {
                m_logger.Info("+LoadTrades_EOD_FREC_OPEN()");

                Message = "";
                if (lastBusinessDate == null)
                {
                    Message = string.Format(INVAID_ARGUREMENT_NULL, "lastBusinessDate");
                    return -1;
                }

                var repository2 = new TradeRepository();

                m_logger.Info("LoadTrades_EOD_FREC_OPEN():+GetTradeListFromTE_FREC_OPEN");
                var tradeList2 = repository2.GetTradeListFromTE_FREC_OPEN(lastBusinessDate);
                m_logger.Info("LoadTrades_EOD_FREC_OPEN():-GetTradeListFromTE_BAGS");

                m_logger.Info("LoadTrades_EOD_FREC_OPEN():+InsertTrades_FREC_OPEN");
                repository2.InsertTrades_FREC_OPEN(tradeList2, lastBusinessDate);
                m_logger.Info("LoadTrades_EOD_FREC_OPEN():-InsertTrades_FREC_OPEN");

                m_logger.Info("-LoadTrades_EOD_FREC_OPEN()");

                return tradeList2.Count;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                Message = string.Format(EXCEPTION_MESSAGE, "Load Trades - EOD - FREC", (ex.InnerException ?? ex).Message);
                m_logger.Error(Message);
            }

            m_logger.Info("-LoadTrades_EOD_FREC_OPEN()");
            return -1;
        }

        public int LoadTrades_EOD_FREC_SETTLED(DateTime lastBusinessDate)
        {
            try
            {
                m_logger.Info("+LoadTrades_EOD_FREC_SETTLED()");

                Message = "";
                if (lastBusinessDate == null)
                {
                    Message = string.Format(INVAID_ARGUREMENT_NULL, "lastBusinessDate");
                    return -1;
                }

                var repository2 = new TradeRepository();

                m_logger.Info("LoadTrades_EOD_FREC_SETTLED():+GetTradeListFromTE_FREC_SETTLED");
                var tradeList2 = repository2.GetTradeListFromTE_FREC_SETTLED(lastBusinessDate);
                m_logger.Info("LoadTrades_EOD_FREC_SETTLED():-GetTradeListFromTE_FREC_SETTLED");

                m_logger.Info("LoadTrades_EOD_FREC_SETTLED():+InsertTrades_FREC_SETTLED");
                repository2.InsertTrades_FREC_SETTLED(tradeList2, lastBusinessDate);
                m_logger.Info("LoadTrades_EOD_FREC_SETTLED():-InsertTrades_FREC_SETTLED");


                m_logger.Info("-LoadTrades_EOD_FREC_SETTLED()");
                return tradeList2.Count;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                Message = string.Format(EXCEPTION_MESSAGE, "Load Trades - EOD - FREC", (ex.InnerException ?? ex).Message);
                m_logger.Error(Message);
            }

            m_logger.Info("-LoadTrades_EOD_FREC_SETTLED()");
            return -1;
        }

        public int LoadTrades_EOD_USTRADE(DateTime lastBusinessDate)
        {
            try
            {
                Message = "";
                if (lastBusinessDate == null)
                {
                    Message = string.Format(INVAID_ARGUREMENT_NULL, "lastBusinessDate");
                    return -1;
                }

                var repository2 = new TradeRepository();
                var tradeList2 = repository2.GetTradeListFromTE_USTRADE(lastBusinessDate);
                repository2.InsertTrades_USTRADE(tradeList2, lastBusinessDate);

                return tradeList2.Count;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                Message = string.Format(EXCEPTION_MESSAGE, "Load Trades - EOD - USTRADE", (ex.InnerException ?? ex).Message);
            }
            return -1;
        }

        public int LoadTrades_EOD_CDEALER(DateTime lastBusinessDate)
        {
            try
            {
                Message = "";
                if (lastBusinessDate == null)
                {
                    Message = string.Format(INVAID_ARGUREMENT_NULL, "lastBusinessDate");
                    return -1;
                }

                var repository2 = new TradeRepository();
                var tradeList2 = repository2.GetTradeListFromTE_CDEALER(lastBusinessDate);
                repository2.InsertTrades_CDEALER(tradeList2, lastBusinessDate);

                return tradeList2.Count;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                Message = string.Format(EXCEPTION_MESSAGE, "Load Trades - EOD - USTRADE", (ex.InnerException ?? ex).Message);
            }
            return -1;
        }
    }
}