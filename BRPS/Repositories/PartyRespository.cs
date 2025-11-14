using BRPS.DAO;
using BRPS.Model;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Data;
using System.Data.Entity;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BRPS.Repositories
{
    public enum PartyCategory
    {
        MKT,
        DEPO,
        DACC,
        SALE,
        SECP
    }

    class PartyRespository : RepositoryBase
    {
        #region const variables

        //private const string Client_Buy_Limit_SG="PAM1";
        //private const string Client_Sell_Limit_SG="PAM2";
        //private const string Client_Margin_Credit_Limit_SG="PAM3";

        private const string KGK_SG_COMAPANY_CODE="CMP1";
        private const string CLIENT_BUY_LIMIT_SG="PI01";
        private const string CLIENT_SELL_LIMIT_SG="PI02";
        private const string CLIENT_MARGIN_CREDIT_LIMIT_SG="PI03";
        private const string CLIENT_SUSPENSE_ACCOUNT="PS01";
        private const string ACCOUNT_UNDER_INFLUENCE_OR_CONTROL_1="PS02";
        private const string ACCOUNT_UNDER_INFLUENCE_OR_CONTROL_2="PS03";
        private const string ACCOUNT_UNDER_INFLUENCE_OR_CONTROL_3="PS04";
        private const string ACCOUNT_UNDER_INFLUENCE_OR_CONTROL_4="PS05";
        private const string ACCOUNT_UNDER_INFLUENCE_OR_CONTROL_5="PS06";
        private const string CLIENT_ASSOCIATED_JOINT_ACCOUNT="PS07";
        private const string LEI="PN01";
        private const string LEI_Name="PN02";
        private const string CLIENT_OCCUPATION="PN03";
        private const string CLIENT_EMPLOYER_NAME="PN04";
        private const string CLIENT_PAID_UP_CAPITAL="PN05";
        private const string CLIENT_BANKRUPTCY_RECORD="PN06";
        private const string CLIENT_DELINQUENCY_RECORD="PN07";
        private const string AUTHORISED_3RD_PTY_T1_NAME="PN08";
        private const string AUTHORISED_3RD_PTY_T1_RELATIONSHIP="PN09";
        private const string AUTHORISED_3RD_PTY_T1_NRIC_PASSPORT="PN10";
        private const string AUTHORISED_3RD_PTY_T1_CONTRACT_NO="PN11";
        private const string AUTHORISED_3RD_PTY_T1_EFFECTIVE_DATE="PN12";
        private const string AUTHORISED_3RD_PTY_T2_NAME="PN13";
        private const string AUTHORISED_3RD_PTY_T2_RELATIONSHIP="PN14";
        private const string AUTHORISED_3RD_PTY_T2_NRIC_PASSPORT="PN15";
        private const string AUTHORISED_3RD_PTY_T2_CONTACT_NO="PN16";
        private const string AUTHORISED_3RD_PTY_T2_EFFECTIVE_DATE="PN17";
        private const string PARTY_3RD_PAYEE_NAME="PN18";
        private const string PARTY_3RD_PAYEE_BANK_AC="PN19";
        private const string PARTY_3RD_PAYEE_BANK_NAME="PN20";
        private const string PARTY_3RD_PAY_AUTHORISATION_EFFECTIVE_DATE="PN21";
        private const string CLIENT_RELATED_AC_RELATIONSHIP="PN22";
        private const string CLIENT_PARENT_COMPANY="PN23";        
        private const string SG_BANK_CODE="SGBC";
        private const string CDP_POSITION_AC_NO_DA_CODE="SGLC";
        private const string CDP_POSITION_ACCOUNT_NAME="PANA";
        private const string KLSE_MEMBER="KLSE";
        private const string SEHK_MEMBER="SEHK";
        private const string SGX_MEMBER="SGX";
        private const string CLIENT_ID_NRIC="PR01";
        private const string CLIENT_ID_PASSPORT_NO="PR02";
        private const string CLIENT_ID_CO_REG_NO="PR03";
        private const string FATCA_US_TAXPAYER_ID_NO="PR04";
        private const string FATCA_EMPLOYEE_ID_NO="PR05";
        private const string CLIENT_KLSE_CDS_AC_CODE="PR06";
        private const string CLIENT_ID_HKID_NO="PR07";
        private const string FATCA_US_TEL_NO="PR08";
        private const string FATCA_GIIN="PR09";
        private const string CLIENT_SCB_IMA_NO_SG="PR10";
        private const string CLIENT_SCB_TRUST_AC_No_SG="PR11";
        private const string SGX_CLIENT_NO = "SXCT";

        private const string CLIENT_DATE_OF_BIRTH_INCORPORATION = "PD01";
        private const string CLIENT_DATE_OF_UPD_PERSONAL_PARTICULARS_FORM = "PD02";
        private const string CLIENT_ACCOUNT_SUSPENSION_DATE = "PD03";
        private const string CLIENT_LAST_TRANSACTION_DATE = "PD04";
        private const string DATE_OF_FATCA_WTH_CERT = "PD05";
        private const string DATE_OF_PASSPORT_EXPIRY = "PD06";
        private const string AC_OPENNING_DATE = "PAOD";        
        private const string AC_EXPIRY_DATE = "PAED";     
        private const string CLIENT_EPS_END_DATE = "EPCD";
        private const string CLIENT_EPS_START_DATE = "EPSD";
        private const string GSA_SUB_AC_START_DATE = "GSCD";
        private const string GSA_SUB_AC_END_DATE = "GSED";
        private const string CDP_SUB_AC_NO = "CDPS";

        private const string CLIENT_STATUS_NARRATIVE = "STAT"; //Added by Jay on 4 Oct 2017

        private const string PARENT_COMPANY = "PCTY";
        private const string SALESPERSON = "SALE";
        private const string COMAPNY = "COMP";
        private const string GST_AUTHORITY = "GSTA";

        private const string NON_TRADING_ACCOUNT_TYPE = "NTRD";

        /// <summary>
        /// account depot alias
        /// </summary>
        private const string DEPOT_ALIAS_EPS = "CPTY EPSG";
        private const string DEPOT_ALIAS_GIRO = "CPTY GIRO";
        private const string DEPOT_ALIAS_CPF = "CPTY SGCPF";
        private const string DEPOT_ALIAS_SRS = "CPTY SGSRS";

        /// <summary>
        /// Salespaerson references
        /// </summary>
        private const string SALESPERSON_BLOOMBERG_REFERENCE = "BMBG";
        private const string SALESPERSON_DEPT_CODE = "DEPT";
        private const string SALESPERSON_GENERIC_EXTERNAL_REFERENCE1 = "SR01";
        private const string SALESPERSON_GENERIC_EXTERNAL_REFERENCE2 = "SR02";
        private const string SALESPERSON_GENERIC_EXTERNAL_REFERENCE3 = "SR03";
        private const string SALESPERSON_GENERIC_EXTERNAL_REFERENCE4 = "SR04";
        private const string SALESPERSON_GENERIC_EXTERNAL_REFERENCE5 = "SR05";
        private const string SALESPERSON_MAS_REP_NO="MASR";
        private const string SALESPERSON_SGX_CODE="SGTR";

        /// <summary>
        /// Salespaerson classifications
        /// </summary>
        private const string SALESPERSON_CLIENT_TITLE = "PC03";
        private const string SALESPERSON_GENERIC_CLASSIFICATION1 = "SC01";

        /// <summary>
        /// Salesperson Associations
        /// </summary>
        private const string SALESPERSON_OWN_TRADING_AC_NO_1="SS01";
        private const string SALESPERSON_OWN_TRADING_AC_NO_2="SS02";
        private const string SALESPERSON_OWN_TRADING_AC_NO_3 = "SS03";	
        private const string SALESPERSON_COST_CENTRE="COST";
        private const string SALESPERSON_BACKUP = "BKSA";

        /// <summary> 
        /// Salesperson Narrative
        /// </summary>
        private const string SALESPERSON_INTERNAL_NARRATIVE="INAM";
        private const string SALESPERSON_INFORMATION_NARRATIVE="INFO";
        private const string SALESPERSON_STATUS_NARRATIVE="STAT";

        /// <summary>
        /// Salesperson Dates
        /// </summary>
        private const string SALESPERSON_JOINING_DATE="SD01";
        private const string SALESPERSON_DEPARTURE_DATE="SD02";
        private const string SALESPERSON_OBTAINING_M6A_DATE = "SD03";

        #endregion

        private BRPSDbContext context = new BRPSDbContext();

        #region Deconstructure
        ~PartyRespository()
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

        private enum PartyClass
        {
            Party_Confirm_Flags=1718,
            Position_Account_Category=2739,
            Title=2174,
            Marital_Status=2175,
            SG_PR=2740,
            //SG_GST_Charge,
            //Issue_Country=2741,
            Residential_Code=2742,
            Financial_Nature=2743,
            Ownership_Category_Code=2744,
            Principal_Business=2745,
            EU_Corporate_Sector=2746,
            Employment_Status=2747,
            Annual_Income=2176,
            Net_Worth=2177,
            Accredited_Investor=2178,
            Risk_Tolerance_Level=2179,
            Trading_Account_Type=2267,
            Client_Type=4002,
            Party_Custody_Type=1782,
            Capacity=2748,
            Deafult_Settle_Curr=2759,
            Account_Status=2749,
            EPS_Link_Status=2765,
            GIRO_Payment_Type=2755,
            GSA_Sub_Account_Link_Status=2754,
            Daily_Statement=2757,
            Monthly_Statement=2758,
            CDP_Position_Account_Origin=2750,
            CDP_Position_Account_Type=2751,
            CDP_Position_Account_Segregation=2752,
            AC_Opening_Doc_Flag =2726,
            Client_High_Risk_Factor_SG=2767,
            Client_Suspension_Reason_Flag=2768,
            Standing_Authority=2769,
            Client_AC_History=2770,
            Salesperson_Generic_Classn1=2222,
            GST_Status = 2181
        }

        public DataSet ExtractPartysFromTEByCategory(PartyCategory partyCategoty, DateTime lastDate)
        {            
            DataSet ds = null;
            using (var dao = new GenericDAO(new OleDbConnectionSettingsProvider()))
            {
                string partyCatFilter = string.Format(" P.party_category_p2k In ('{0}') ", partyCategoty);
                //string partySegregation = string.Format(" PAP.assoc_party_ref_p2k In ('{0}') ", "SEGPARTY1");

                string filters = string.Format(" WHERE {0} and convert(CHAR(8),P.version_date,112)>='{1:yyyyMMdd}'\n ", partyCatFilter, lastDate);

                var ssql= new StringBuilder( "SELECT party_ref_p2k, party_short_name_p2k,party_extra_long_name= party_extra_long_name1_wil+party_extra_long_name2_wil,"  ); 
                ssql.Append( " party_category_p2k,base_instr_p2k, holiday_id_p2k, party_country_p2k, party_location_p2k, active_ind_p2k,  version_date ,version_no, ");
                ssql.Append(" joint_account=rtrim(isnull((SELECT assoc_party_ref_p2k FROM party_assoc_p2k A WHERE A.originating_party_ref_p2k=P.party_ref_p2k and assoc_code_p2k='PS07'), '')) ");
                ssql.Append( " FROM party_p2k P " );
                ssql.Append(filters);
                ssql.Append(" and P.party_ref_p2k IN (SELECT P.party_ref_p2k FROM party_assoc_p2k AS PAP WHERE PAP.originating_party_ref_p2k = P.party_ref_p2k and PAP.assoc_party_ref_p2k In ('SEGPARTY1'))");
                ssql.Append(" Order by joint_account ");

                ssql.Append("SELECT C.party_ref_p2k, C.classification_type_p2k, C.class_p2k FROM party_classification_wil C INNER JOIN party_p2k P ON C.party_ref_p2k=P.party_ref_p2k ");
                ssql.Append( filters );

                ssql.Append("SELECT N.party_ref_p2k, N.narrative_code_wil,N.narrative_wil FROM party_narrative_wil N INNER JOIN party_p2k P ON N.party_ref_p2k=P.party_ref_p2k ");
                ssql.Append(filters);

                ssql.Append("SELECT F.party_ref_p2k, F.party_class_wil, F.party_code_wil FROM party_flag_wil F INNER JOIN party_p2k P ON F.party_ref_p2k=P.party_ref_p2k ");
                ssql.Append( filters );

                ssql.Append("SELECT D.party_ref_p2k, date_type_wil, date_wil FROM party_date_wil D INNER JOIN party_p2k P ON D.party_ref_p2k=P.party_ref_p2k ");
                ssql.Append(filters);

                ssql.Append("SELECT M.party_ref_p2k, instr_type_wil, p2000_instr_ref_p2k , quantity_wil FROM party_amount_wil M INNER JOIN party_p2k P ON M.party_ref_p2k=P.party_ref_p2k ");
                ssql.Append(filters);
                
                ssql.Append("SELECT R.party_ref_p2k,ext_service_code_p2k, R.ext_party_ref_p2k FROM party_ext_ref_p2k R INNER JOIN party_p2k P ON R.party_ref_p2k=P.party_ref_p2k ");
                ssql.Append(filters);

                ssql.Append("SELECT AC.originating_party_ref_p2k as party_ref_p2k,  AC.assoc_code_p2k,AC.assoc_party_ref_p2k FROM party_assoc_p2k AC INNER JOIN party_p2k P " );
                //ssql.Append(" ON AC.originating_party_ref_p2k=P.party_ref_p2k AND AC.assoc_party_ref_p2k = 'SEGPARTY2'");
                ssql.Append(" ON AC.originating_party_ref_p2k=P.party_ref_p2k ");
                ssql.Append(filters);

                ssql.Append(" Select A.party_ref_p2k, address_code_p2k, contact_name_p2k, contact_title_p2k, address_1_p2k, address_2_p2k, address_3_p2k, address_4_p2k,");
                ssql.Append(" address_5_p2k, post_code_p2k, telephone_no_p2k, fax_no_p2k, email_wil, A.version_date ,A.version_no ");
                ssql.Append(" from party_address_p2k A INNER JOIN party_p2k P ON A.party_ref_p2k=P.party_ref_p2k ");
                ssql.Append(filters);

                if (partyCategoty == PartyCategory.SECP)
                {
                    ssql.Append("SELECT D.party_ref_p2k, D.depot_alias_wil, D.account_number_wil, D.account_narrative_1_wil, D.account_name_wil," +
                                "(SELECT party_short_name_p2k FROM party_p2k AS PP where PP.party_ref_p2k = daw.depot_ref_p2k) AS EPSBankName FROM depot_account_wil D ");
                    ssql.Append("INNER JOIN party_p2k P ON D.party_ref_p2k=P.party_ref_p2k ");
                    ssql.Append("INNER JOIN depot_alias_wil daw ON daw.party_ref_p2k=P.party_ref_p2k and daw.depot_alias_wil = 'CPTY EPSG'");
                    ssql.Append(filters);
                    ssql.AppendFormat(" and D.depot_alias_wil IN ('{0}','{1}','{2}','{3}') ", DEPOT_ALIAS_EPS, DEPOT_ALIAS_GIRO, DEPOT_ALIAS_SRS, DEPOT_ALIAS_CPF);
                }
             
                ds= dao.ExecuteQuery(ssql.ToString());                
                ds.Relations.Add(new DataRelation("Party_Classifications", ds.Tables[0].Columns[0], ds.Tables[1].Columns[0],false));
                ds.Relations.Add(new DataRelation("Party_Narratives", ds.Tables[0].Columns[0], ds.Tables[2].Columns[0],false));
                ds.Relations.Add(new DataRelation("Party_Flags", ds.Tables[0].Columns[0], ds.Tables[3].Columns[0],false));
                ds.Relations.Add(new DataRelation("Party_Dates", ds.Tables[0].Columns[0], ds.Tables[4].Columns[0],false));
                ds.Relations.Add(new DataRelation("Party_Amounts", ds.Tables[0].Columns[0], ds.Tables[5].Columns[0],false));
                ds.Relations.Add(new DataRelation("Party_References", ds.Tables[0].Columns[0], ds.Tables[6].Columns[0],false));
                ds.Relations.Add(new DataRelation("Party_Assocs", ds.Tables[0].Columns[0], ds.Tables[7].Columns[0],false));
                ds.Relations.Add(new DataRelation("Party_Addresses", ds.Tables[0].Columns[0], ds.Tables[8].Columns[0],false));
                
                if (partyCategoty == PartyCategory.SECP)
                {
                    ds.Relations.Add(new DataRelation("Depot_Accounts", ds.Tables[0].Columns[0], ds.Tables[9].Columns[0],false));
                }
            }
            return ds;
        }

        public List<Market> GetAllMarkets()
        {
            using (var context = new BRPSDbContext())
            {
                return context.Markets.ToList();
            }
        }

        #region save parties to local repository

        public int ReloadAllParties(PartyCategory partyCategory,  List<Party> parties)
        {
            try
            {    
                //This will cascade delete Client, SalesPerson, Address
                context.Database.ExecuteSqlCommand(string.Format("delete Party where Party_Category='{0}'", partyCategory));
                if (partyCategory == PartyCategory.SECP)
                {
                    context.Database.ExecuteSqlCommand("delete Client");
                    context.Database.ExecuteSqlCommand("delete ClientAccount");
                }
                InsertEntities<Party>(context, parties,2000);
                return parties.Count;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.StackTrace);
                throw new Exception("Error in ReloadAllParties: " + ex.Message, ex.InnerException == null ? ex : ex.InnerException);
            }
        }

        public int UpdateParties(List<Party> parties)
        {
            try
            {
                int records = 0;
                foreach (var editParty in parties)
                {
                    var dbParty = context.Parties.SingleOrDefault(c => c.Party_Ref==editParty.Party_Ref);
                    if (dbParty==null)
                    {
                        if (editParty.Client != null)
                        {
                            editParty.Client.Status = "N";
                        }
                        context.Parties.Add(editParty);
                        records++;
                    }
                    else
                    {
                        if (dbParty.Version_No != editParty.Version_No)
                        {
                            records++;
                            
                            var dbEntity = context.Entry<Party>(dbParty);
                            dbEntity.CurrentValues.SetValues(editParty);
                            dbEntity.State = EntityState.Modified;
                            var editClient = editParty.Client;
                            if (editClient != null)
                            {
                                editClient.Status = "A";  
                                var dbentry = context.Entry<Client>(dbParty.Client);
                                dbentry.CurrentValues.SetValues(editClient);                                                             
                                dbentry.State = EntityState.Modified;

                                if (editClient.ClientAccount != null)
                                {
                                    var acctEntry = context.Entry<ClientAccount>(dbParty.Client.ClientAccount);
                                    acctEntry.CurrentValues.SetValues(editClient.ClientAccount);
                                    acctEntry.State = EntityState.Modified;
                                }
                            }

                            foreach (var address in editParty.Addresses)
                            {
                                var dbAddress = dbParty.Addresses.FirstOrDefault(a => a.Party_Ref == address.Party_Ref && a.Address_Code == address.Address_Code);
                                if (dbAddress == null)
                                {
                                    dbParty.Addresses.Add(address);
                                    address.Party = dbParty;                                   
                                }
                                else if (dbAddress.Version_No != address.Version_No)
                                {
                                    var addrEntity = context.Entry<Address>(dbAddress);
                                    addrEntity.CurrentValues.SetValues(address);
                                    addrEntity.State = EntityState.Modified;
                                }
                            }

                            var deletedAddrList = new List<Address>();
                            foreach (var dbAddress in dbParty.Addresses)
                            {
                                var editAddress = editParty.Addresses.FirstOrDefault(a => a.Party_Ref == dbAddress.Party_Ref && a.Address_Code == dbAddress.Address_Code);
                                if (editAddress == null)
                                {
                                    deletedAddrList.Add(dbAddress);                                       
                                }
                            }

                            foreach (var addr in deletedAddrList)
                            {
                                dbParty.Addresses.Remove(addr);
                                context.Addresses.Remove(addr);
                            }                                
                        }
                    }                    
                }

                if (records > 0)
                {
                    SaveChanges(context);
                }

                return records;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.StackTrace);
                throw new Exception("Error in UpdateClients: " + ex.Message, ex.InnerException == null ? ex : ex.InnerException);
            }
        }

        #endregion


        #region GetPartyListFromGloss methods

        public List<Party> GetPartyListFromGloss(PartyCategory partyCategoty, DateTime lastDate)
        {
            var ds = ExtractPartysFromTEByCategory(partyCategoty, lastDate);
            if (ds == null || ds.Tables.Count == 0)
            {
                return null;
            }

            var Partys = new List<Party>();          
            foreach (DataRow row in ds.Tables[0].Rows)
            {
                var party=new Party(){
                    Party_Ref=row["party_ref_p2k"].ToString().TrimEnd(),
                    Party_Category= row["party_category_p2k"].ToString().TrimEnd(),
                    Company = KGK_SG_COMAPANY_CODE,
                    Version_Date =Convert.ToDateTime( row["version_date"]),
                    Version_No = Convert.ToInt32(row["version_no"]),
                };

                if (partyCategoty == PartyCategory.SECP)
                {
                    party.Client = new Client()
                    {
                        Client_Number = party.Party_Ref,
                        Short_Name = row["party_short_name_p2k"].ToString().TrimEnd(),
                        Long_Name = row["party_extra_long_name"].ToString().TrimEnd(),
                        Location = row["party_location_p2k"].ToString().TrimEnd(),
                        Country = row["party_country_p2k"].ToString().TrimEnd(),
                        Version_Date = party.Version_Date,
                        Version_No = party.Version_No,
                        Client_Status = (row["active_ind_p2k"].Equals("A") ? "0" : "1"),
                        Party = party,
                        Client_Account_Number = row["joint_account"].ToString()=="" ? party.Party_Ref : row["joint_account"].ToString(),
                    };

                    var client = party.Client;
                    var account = new ClientAccount()
                    {
                        Client_Account_Number = client.Client_Account_Number,
                        Account_Status = (row["active_ind_p2k"].Equals("A") ? "0" : "1"),
                    };

                    account.Clients.Add(client);                    
                    if (client.Trading_Account_Type != NON_TRADING_ACCOUNT_TYPE)
                    {
                        client.ClientAccount = account;
                    }
                }
                else if ( partyCategoty == PartyCategory.SALE)
                {
                    party.Salesperson = new Salesperson()
                    {
                        Salesperson_Ref = party.Party_Ref,
                        Short_Name = row["party_short_name_p2k"].ToString().TrimEnd(),
                        Long_Name = row["party_extra_long_name"].ToString().TrimEnd(),
                        Holiday_Calendar = row["holiday_id_p2k"].ToString().TrimEnd(),
                        Country = row["party_country_p2k"].ToString().TrimEnd(),
                        Location = row["party_location_p2k"].ToString().TrimEnd(),
                        Version_Date = party.Version_Date,
                        Version_No = party.Version_No,
                        Active = row["active_ind_p2k"].ToString().TrimEnd(),
                        Party = party,
                    };
                }

                PopulatePartyAssocs(row, party);
                if (party.Company != KGK_SG_COMAPANY_CODE)
                {
                    continue;
                }
                PopulatePartyClassifications(row, party);
                PopulatePartyNarratives(row, party);                
                PopulatePartyDates(row, party);                
                
                PopulatePartyReferences(row, party);
                PopulatePartyAddresses(row, party);

                if (GetPartyCategory(party) == PartyCategory.SECP)
                {
                    PopulateClientFlags(row, party.Client);
                    PopulatePartyAmounts(row, party.Client);
                    PopulateClientDepotAccounts(row, party.Client.ClientAccount);

                    if (party.Client.Trading_Account_Type == NON_TRADING_ACCOUNT_TYPE)
                    {
                        party.Client.ClientAccount = null;
                    }
                }

                Partys.Add(party);
            }

            return Partys;
        }
        
        private void PopulateClientDepotAccounts(DataRow row, ClientAccount account)
        {
            foreach (var acctRow in row.GetChildRows("Depot_Accounts"))
            {
                var depotAlias=acctRow["depot_alias_wil"].ToString().TrimEnd();
                var account_Number = acctRow["account_number_wil"].ToString().TrimEnd();
                var bank_code=acctRow["account_narrative_1_wil"].ToString().TrimEnd();
                var eps_bankname = acctRow["EPSBankName"].ToString().TrimEnd();

                switch (depotAlias)
                {
                    case DEPOT_ALIAS_EPS:
                        account.EPS_Bank_No = bank_code;
                        account.EPS_Bank_Acct = account_Number;
                        account.EPS_Bank_Name = eps_bankname;
                        break;
                    case DEPOT_ALIAS_GIRO:
                        if (account_Number.Length > 4)
                        {
                            account.GIRO_Bank_No = bank_code == "" ? account_Number.Substring(0, 4) : bank_code;
                            account.GIRO_Bank_Acct = bank_code == "" ? account_Number.Substring(4) : account_Number;
                        }
                        break;
                    case DEPOT_ALIAS_SRS:
                        if (account_Number.Length > 4)
                        {
                            account.SRS_Bank_No = bank_code == "" ? account_Number.Substring(0, 3) : bank_code;
                            account.SRS_Bank_Acct = bank_code == "" ? account_Number.Substring(3) : account_Number;
                        }
                        break;
                    case DEPOT_ALIAS_CPF:
                        if (account_Number.Length > 4)
                        {
                            account.CPF_Bank_No = bank_code == "" ? account_Number.Substring(0, 3) : bank_code;
                            account.CPF_Bank_Acct = bank_code == "" ? account_Number.Substring(3) : account_Number;
                        }
                        break;
                    default:
                        break;
                }
            }
        }

        private static void PopulatePartyReferences(DataRow row, Party party)
        {
            var salesperson = party.Salesperson;
            var client = party.Client; 
            var account = GetClientAccount(party);
            
            foreach (var refRow in row.GetChildRows("Party_References"))
            {
                var data = refRow["ext_party_ref_p2k"].ToString().TrimEnd();
                var refCode = refRow["ext_service_code_p2k"].ToString().TrimEnd();
                if(GetPartyCategory(party)==PartyCategory.SECP)
                {
                    switch (refCode)
                    {
                        case CLIENT_ID_NRIC:
                            client.Unique_ID = data;
                            client.Identification_Type = "1";
                            break;
                        case CLIENT_ID_PASSPORT_NO:
                            client.Unique_ID = data;
                            client.Identification_Type = "2";
                            break;
                        case CLIENT_ID_CO_REG_NO:
                            client.Unique_ID = data;
                            client.Identification_Type = "3";
                            break;
                        case CDP_SUB_AC_NO:
                            account.Securities_Account= data;
                            break;
                        case CDP_POSITION_AC_NO_DA_CODE:
                            account.Position_Account_Number = data;
                            break;
                        case CLIENT_KLSE_CDS_AC_CODE:
                            account.KLSE_CDS_Account_Code = data;
                            break;
                        case CLIENT_SCB_TRUST_AC_No_SG:
                            account.SCB_TRUST_AC_NO_SG = data;
                            break;
                        case CLIENT_SCB_IMA_NO_SG:
                            account.SCB_IMA_NO_SG = data;
                            break;
                        case SGX_CLIENT_NO:
                            account.SGX_Client_No = data;
                            break;
                        default:
                            break;
                    }
                }
                else if (GetPartyCategory(party) == PartyCategory.SALE)
                {
                    switch (refCode)
                    {
                        case SALESPERSON_BLOOMBERG_REFERENCE:
                            salesperson.Bloomberg_Ref = data;
                            break;
                        case SALESPERSON_DEPT_CODE:
                            salesperson.Dept_Code = data;
                            break;
                        case SALESPERSON_GENERIC_EXTERNAL_REFERENCE1:
                            salesperson.Sales_Group = data;
                            break;
                        case SALESPERSON_GENERIC_EXTERNAL_REFERENCE2:
                            salesperson.GL_User_ID = data;
                            break;
                        case SALESPERSON_GENERIC_EXTERNAL_REFERENCE3:
                            salesperson.Salesperson_Ref_1 = data;
                            break;
                        case SALESPERSON_GENERIC_EXTERNAL_REFERENCE4:
                            salesperson.Salesperson_Ref_2 = data;
                            break;
                        case SALESPERSON_GENERIC_EXTERNAL_REFERENCE5:
                            salesperson.Salesperson_Ref_3 = data;
                            break;
                        case SALESPERSON_MAS_REP_NO:
                            salesperson.MAS_Rep_No = data;
                            break;
                        case SALESPERSON_SGX_CODE:
                            salesperson.SGX_TR_Code = data;
                            break;
                        default:
                            break;
                    }
                }
                else
                {
                    throw new NotImplementedException();
                }
            }
        }

        private static void PopulatePartyAssocs(DataRow row, Party party)
        {
            var salesperson = party.Salesperson;
            var client = party.Client;
            var account = GetClientAccount(party);
            foreach (var assoRow in row.GetChildRows("Party_Assocs"))
            {
                var data = assoRow["assoc_party_ref_p2k"].ToString().TrimEnd();
                var assocCode = assoRow["assoc_code_p2k"].ToString().TrimEnd();
                if (GetPartyCategory(party) == PartyCategory.SECP)
                {
                    switch (assocCode)
                    {
                        case COMAPNY:
                            party.Company = data;
                            break;
                        case SALESPERSON:
                            account.Remisier_Code = data;
                            break;
                        case PARENT_COMPANY:
                            account.Parent_Account = data;
                            break;
                        default:
                            break;
                    }
                }
                else if (GetPartyCategory(party) == PartyCategory.SALE)
                {
                    switch (assocCode)
                    {
                        case COMAPNY:
                            party.Company = data;
                            break;
                        case SALESPERSON_OWN_TRADING_AC_NO_1:
                            salesperson.Own_Trading_AC_No_1 = data;
                            break;
                        case SALESPERSON_OWN_TRADING_AC_NO_2:
                            salesperson.Own_Trading_AC_No_2 = data;
                            break;
                        case SALESPERSON_OWN_TRADING_AC_NO_3:
                            salesperson.Own_Trading_AC_No_3 = data;
                            break;
                        case SALESPERSON_BACKUP:
                            salesperson.Backup_Salesperson = data;
                            break;
                        case SALESPERSON_COST_CENTRE:
                            salesperson.Cost_Centre = data;
                            break;
                        default:
                            break;
                    }
                }
                else
                {
                    throw new NotImplementedException();
                }
            }
        }

        private static void PopulatePartyAmounts(DataRow row, Client client)
        {
            var account = client.ClientAccount;
            foreach (var amtRow in row.GetChildRows("Party_Amounts"))
            {
                var data = Convert.ToDecimal(amtRow["quantity_wil"]);
                switch (amtRow["instr_type_wil"].ToString().TrimEnd())
                {
                    case CLIENT_BUY_LIMIT_SG:
                        account.Buy_Limit = data;
                        break;
                    case CLIENT_SELL_LIMIT_SG:
                        account.Sell_Limit = data;
                        break;
                    case CLIENT_MARGIN_CREDIT_LIMIT_SG:
                        account.Margin_Credit_Limit = data;
                        break;
                }
            }
        }

        private static void PopulatePartyDates(DataRow row, Party party)
        {
            var salesperson = party.Salesperson;
            var client = party.Client; 
            var account = GetClientAccount(party);
            foreach (var dtRow in row.GetChildRows("Party_Dates"))
            {
                var date =Convert.ToDateTime(dtRow["date_wil"]).Date;
                var dtCode = dtRow["date_type_wil"].ToString().TrimEnd();
                if (GetPartyCategory(party) == PartyCategory.SECP)
                {
                    switch (dtCode)
                    {
                        case CLIENT_DATE_OF_BIRTH_INCORPORATION:
                            client.Birth_Incorp_Date = date;
                            break;
                        case CLIENT_ACCOUNT_SUSPENSION_DATE:
                            account.Suspended_Date = date;
                            break;
                        case CLIENT_LAST_TRANSACTION_DATE:
                            account.Last_Transction_Date = date;
                            break;
                        case AC_OPENNING_DATE:
                            account.Activated_Date = date;
                            break;
                        case AC_EXPIRY_DATE:
                            account.Closed_Date = date;
                            break;
                        case CLIENT_EPS_END_DATE:
                            account.EPS_Close_Date = date;
                            break;
                        case CLIENT_EPS_START_DATE:
                            account.EPS_Commence_Date = date;
                            break;
                        case GSA_SUB_AC_START_DATE:
                            account.CDP_Acct_Linkage_Date = date;
                            break;
                        case GSA_SUB_AC_END_DATE:
                            account.CDP_Acct_Linkage_Revoc_Date = date;
                            break;
                       // default:
                         //   account.Suspended_Date = DateTime.UtcNow;
                         //   account.Last_Transction_Date = DateTime.UtcNow;
                         //   account.Activated_Date = DateTime.UtcNow;
                         //   account.Closed_Date = DateTime.UtcNow;
                        //    account.EPS_Close_Date = DateTime.UtcNow;
                        //    account.CDP_Acct_Linkage_Date = DateTime.UtcNow;
                        //    account.CDP_Acct_Linkage_Revoc_Date = DateTime.UtcNow;
                            break;
                    }
                }
                else if (GetPartyCategory(party) == PartyCategory.SALE)
                {
                    switch (dtCode)
                    {
                        case SALESPERSON_JOINING_DATE:
                            salesperson.Joining_Date = date;
                            break;
                        case SALESPERSON_OBTAINING_M6A_DATE:
                            salesperson.Obtaining_M6A_Date = date;
                            break;
                        case SALESPERSON_DEPARTURE_DATE:
                            salesperson.Departure_Date = date;
                            break;
                        default:
                            break;
                    }
                }
                else
                {
                    throw new NotImplementedException();
                }
            } 
        }

        private static void PopulateClientFlags(DataRow row, Client client)
        {
            var account = client.ClientAccount;
            foreach (var flgRow in row.GetChildRows("Party_Flags"))
            {
                var data = flgRow["party_code_wil"].ToString().TrimEnd();
                switch ((PartyClass)flgRow["party_class_wil"])
                {
                    case PartyClass.Client_High_Risk_Factor_SG:
                        if (string.IsNullOrEmpty(client.High_Risk_Factors))
                        {
                            client.High_Risk_Factors = data; ;
                        }
                        else
                        {
                            client.High_Risk_Factors += ("|" + data);
                        }                                                       
                        break;
                    case PartyClass.AC_Opening_Doc_Flag:
                        if (string.IsNullOrEmpty(client.AC_Openning_Doc))
                        {
                            client.AC_Openning_Doc = data ;
                        }
                        else
                        {
                            client.AC_Openning_Doc += ( "|" + data);
                        }
                        if (data == "09")
                        {
                            account.Is_Truth_Account = true;
                        }
                        else if (data == "10")
                        {
                            account.Online_Eligible = true;
                        }
                        break;
                    case PartyClass.Client_Suspension_Reason_Flag:
                        if(string.IsNullOrEmpty(account.Suspension_Reasons))
                        {
                            account.Suspension_Reasons = data ;
                        }
                        else
                        {
                            account.Suspension_Reasons += ("|" + data );
                        }
                        break;
                    case PartyClass.Party_Confirm_Flags:
                        account.Trade_Confirmation = "x";//Jay commented
                        break;
                    case PartyClass.Client_AC_History:
                        //client.Account_History = data;
                        break;
                    case PartyClass.Standing_Authority:
                        if (string.IsNullOrEmpty(client.Standing_Authority))
                        {
                            client.Standing_Authority = data;
                        }
                        else
                        {
                            client.Standing_Authority += ( "|" + data );
                        }
                        break;
                    default:
                        break;
                }
            }
        }

        private static void PopulatePartyNarratives(DataRow row, Party party)
        {
            var salesperson = party.Salesperson;
            var client = party.Client;
            var account = GetClientAccount(party);           
            foreach (var narrRow in row.GetChildRows("Party_Narratives"))
            {
                var data = narrRow["narrative_wil"].ToString().TrimEnd();
                var narrCode=narrRow["narrative_code_wil"].ToString().TrimEnd();
                if (GetPartyCategory(party) == PartyCategory.SECP)
                {
                    switch (narrCode)
                    {
                        case LEI:
                            client.LEI = data;
                            break;
                        case LEI_Name:
                            client.LEI = data;
                            break;
                        case CLIENT_OCCUPATION:
                            client.Occupation = data.Length > 30 ? data.Substring(0, 30) : data;
                            break;
                        case CLIENT_EMPLOYER_NAME:
                            client.Employer_Name = data;
                            break;
                        case CLIENT_PAID_UP_CAPITAL:
                            client.Paidup_Captital = data;
                            break;
                        case CLIENT_BANKRUPTCY_RECORD:
                            //client.Bankruptcy_Record = data;
                            break;
                        case CLIENT_DELINQUENCY_RECORD:
                            //client.Delinquency_Record = data;
                            break;
                        case CLIENT_RELATED_AC_RELATIONSHIP:
                            account.Relationship = data;
                            break;
                        case CLIENT_STATUS_NARRATIVE:
                            account.Status_Narrative = data;
                            break;
                        case CDP_POSITION_ACCOUNT_NAME:
                        //account.Position_Account_Name = data;
                        default:
                            break;
                    }
                }
                else if (GetPartyCategory(party) == PartyCategory.SALE)
                {
                    switch (narrCode)
                    {
                        case SALESPERSON_INFORMATION_NARRATIVE:
                            salesperson.Info_Narrative = data;
                            break;
                        case SALESPERSON_INTERNAL_NARRATIVE:
                            salesperson.Internal_Narrative = data;
                            break;
                        case SALESPERSON_STATUS_NARRATIVE:
                            salesperson.Status_Narrative = data;
                            break;
                        default:
                            break;
                    }
                }
                else
                {
                    throw new NotImplementedException();
                }
            }
        }

        private static void PopulatePartyClassifications(DataRow row, Party party)
        {
            var salesperson = party.Salesperson;
            var client = party.Client;
            var account = GetClientAccount(party);
            foreach (var clsRow in row.GetChildRows("Party_Classifications"))
            {
                var data = clsRow["class_p2k"].ToString().TrimEnd();
                var partyClass=(PartyClass)Convert.ToInt32(clsRow["classification_type_p2k"]);

                if (GetPartyCategory(party)==PartyCategory.SECP)
                {
                    switch (partyClass)
                    {
                        case PartyClass.Position_Account_Category:
                            account.Position_Account_Category = data;
                            break;
                        case PartyClass.Title:
                            client.Client_Title = data;
                            break;
                        case PartyClass.Marital_Status:
                            client.Marital_Status = data;
                            break;
                        case PartyClass.SG_PR:
                            client.SG_PR = data.Length == 0 ? "N" : data.Substring(0, 1);
                            break;
                        case PartyClass.Residential_Code:
                            client.Issuing_Country = data;
                            client.Residence_Code = data;
                            break;
                        case PartyClass.Financial_Nature:
                            client.Financial_nature = data;
                            break;
                        case PartyClass.Ownership_Category_Code:
                            client.Ownership_Category_Code = data;
                            break;
                        case PartyClass.Principal_Business:
                            client.Principal_Business = data;
                            break;
                        case PartyClass.EU_Corporate_Sector:
                            client.EU_Corporate_Sector = data;
                            break;
                        case PartyClass.Employment_Status:
                            client.Employment_Status = data;
                            break;
                        case PartyClass.Annual_Income:
                            client.Annual_Income = data;
                            break;
                        case PartyClass.Net_Worth:
                            client.Net_Worth = data;
                            break;
                        case PartyClass.Accredited_Investor:
                            client.Accredited_Investor = data;
                            break;
                        case PartyClass.Risk_Tolerance_Level:
                            client.Risk_Tolerance = data;
                            break;
                        case PartyClass.Trading_Account_Type:
                            client.Trading_Account_Type = data;
                            break;
                        case PartyClass.Client_Type:
                            client.Client_Type = data;
                            break;
                        case PartyClass.Party_Custody_Type:
                            account.Party_Custody_Type = data;
                            break;
                        case PartyClass.Capacity:
                            account.Capacity = data;
                            break;
                        case PartyClass.Deafult_Settle_Curr:
                            account.Default_Settle_Curr = data;
                            break;
                        case PartyClass.Account_Status:
                            account.Account_Status = data;
                            break;
                        case PartyClass.EPS_Link_Status:
                            account.EPS_Link_Status = data;
                            break;
                        case PartyClass.GIRO_Payment_Type:
                            account.GIRO_Payment_Type = data;
                            break;
                        case PartyClass.GSA_Sub_Account_Link_Status:
                            account.CDP_Acct_Linkage_Status = data;
                            break;
                        case PartyClass.CDP_Position_Account_Origin:
                            account.Position_Account_Origin = data;
                            break;
                        case PartyClass.CDP_Position_Account_Type:
                            account.Position_Account_Type = data;
                            break;
                        case PartyClass.CDP_Position_Account_Segregation:
                            account.Segregation = data;
                            break;
                        case PartyClass.GST_Status:
                            account.GST_Status_Code = data;
                            break;
                        default:
                            account.Default_Settle_Curr = "AAA";//Jay added
                            //account.GST_Status_Code = "NO";
                            account.Trade_Confirmation = "";
                            account.CDP_Acct_Linkage_Status = "";
                            account.Client_DA_Code = "";
                            break;
                    }
                }
                else if (GetPartyCategory(party) == PartyCategory.SALE)
                {
                    switch (partyClass)
                    {
                        case PartyClass.Title:
                            salesperson.Title= data;
                            break;
                        case PartyClass.Salesperson_Generic_Classn1:
                            salesperson.Salesperson_Category = data;
                            break;
                        default:
                            break;
                    }
                }
                else
                {
                    throw new NotImplementedException();
                }
            }
        }

        private static void PopulatePartyAddresses(DataRow row, Party party)
        {
            foreach (DataRow addrRow in row.GetChildRows("Party_Addresses"))
            {
                var address = new Address()
                {
                    Party=party,
                    Party_Ref = addrRow["party_ref_p2k"].ToString().TrimEnd(),
                    Address_Code = addrRow["address_code_p2k"].ToString().TrimEnd(),
                    Contact_Name = addrRow["contact_name_p2k"].ToString().TrimEnd(),
                    Contact_Title = addrRow["contact_title_p2k"].ToString().TrimEnd(),
                    Address_1 = addrRow["address_1_p2k"].ToString().TrimEnd(),
                    Address_2 = addrRow["address_2_p2k"].ToString().TrimEnd(),
                    Address_3 = addrRow["address_3_p2k"].ToString().TrimEnd(),
                    Address_4 = addrRow["address_4_p2k"].ToString().TrimEnd(),
                    Address_5 = addrRow["address_5_p2k"].ToString().TrimEnd(),
                    Post_Code = addrRow["post_code_p2k"].ToString().TrimEnd(),
                    Telephone_No = addrRow["telephone_no_p2k"].ToString().TrimEnd(),
                    Fax = addrRow["fax_no_p2k"].ToString().TrimEnd(),
                    Email = addrRow["email_wil"].ToString().TrimEnd(),
                    Version_Date = Convert.ToDateTime(addrRow["version_date"]),
                    Version_No = Convert.ToInt32(addrRow["version_no"])
                };
                party.Addresses.Add(address);
            }
        }

        #endregion

        public static PartyCategory GetPartyCategory( Party party)
        {
            PartyCategory category;
            Enum.TryParse<PartyCategory>(party.Party_Category, out category);
            return category;
        }

        public static ClientAccount GetClientAccount(Party party)
        {
            if (party.Client != null)
            {
                return party.Client.ClientAccount;
            }
            return null;
        }
    }
}
