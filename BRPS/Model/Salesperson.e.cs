using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BRPS.Model
{
    public partial class Salesperson
    {
        public Salesperson()
        {
            Initialize();
        }

        public void Initialize()
        {
            this.Title = "7";
            this.Reporting_Curr = "";
            this.Template = false;
            this.Bloomberg_Ref = "";
            this.GL_User_ID = "";
            this.Salesperson_Ref_1 = "";
            this.Salesperson_Ref_2 = "";
            this.Salesperson_Ref_3 = "";
            this.MAS_Rep_No = "";
            this.SGX_TR_Code = "";
            this.Salesperson_Category = "";
            this.Joining_Date = new DateTime(627667488000000000, DateTimeKind.Unspecified);
            this.Info_Narrative = "";
        }
    }
}
