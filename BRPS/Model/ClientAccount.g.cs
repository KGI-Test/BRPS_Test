using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BRPS.Model
{
    public partial class ClientAccount
    {
        public void Initialize()
        {
            this.Position_Account_Number = "";
            this.Position_Account_Category = "1";
            this.Position_Account_Origin = "C";
            this.Account_Status = "A";
            this.Position_Account_Type = "L";
            this.Party_Custody_Type = "";
            this.Remisier_Code = "";
            this.Created_Date = new DateTime(627667488000000000, DateTimeKind.Unspecified);
            this.Default_Settle_Curr = "AAA";
            this.Contra_Eligible = true;
            this.GST_Status_Code = "1";
            this.Payment_Mode = "";
            this.EPS_Link_Status = "";
            this.EPS_Bank_No = "";
            this.EPS_Bank_Acct = "";
            this.GIRO_Payment_Type = "";
            this.GIRO_Bank_No = "";
            this.GIRO_Bank_Acct = "";
            this.SRS_Bank_No = "";
            this.SRS_Bank_Acct = "";
            this.CPF_Bank_No = "";
            this.CPF_Bank_Acct = "";
            this.Trade_Confirmation = "";
            this.CDP_Acct_Linkage_Status = "";
            this.CDP_Acct_Linkage_Date = new DateTime(627667488000000000, DateTimeKind.Unspecified);
            this.Client_DA_Code = "";
            this.Capacity = "1";
            this.Segregation = "0";
        }
    }
}
