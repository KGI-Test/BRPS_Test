using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BRPS.Model
{
    public partial class Client
    {
        public Client()
        {
            Initialize();
        }

        public void Initialize()
        {
            this.Client_Title = "7";
            this.Marital_Status = "O";
            this.SG_PR = "N";
            this.Identification_Type = "1";
            this.Unique_ID = "";
            this.Issuing_Country = "";
            this.Ownership_Category_Code = "2";
            this.Principal_Business = "";
            this.Residence_Code = "";
            this.Employment_Status = "1";
            this.EU_Corporate_Sector = "";
            this.Financial_nature = "N";
            this.Annual_Income = "7";
            this.Net_Worth = "1";
            this.Accredited_Investor = "";
            this.Risk_Tolerance = "";
            this.High_Risk_Factors = "";
            this.Trading_Account_Type = "CASH";
            this.Client_Type = "";
            this.Status = "";
            this.Client_Status = "0";
        }
    }
}
