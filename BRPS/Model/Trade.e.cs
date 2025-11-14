using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BRPS.Model
{
    public partial class Trade
    {
        public Trade()
        {
            Initialize();
        }

        public void Initialize()
        {
            Instrument = "";
            Entry_Ind="";
            Settle_Status = "N";
            Trade_Status = "N";
            PSMS_Source="";
            SGX_Trade_No="";
            With_Reference = "";
            Execution_Number = "";
        }
    }
}
