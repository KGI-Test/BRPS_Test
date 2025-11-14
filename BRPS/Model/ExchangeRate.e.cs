using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BRPS.Model
{
    public partial class ExchangeRate
    {
        public ExchangeRate()
        {
            Initialize();
        }

        public void Initialize()
        {
            this.CCYPairID = "";
            this.Instr_Ref = "";
            this.Quote_Instr_Ref = "";
            this.Mid_Price = 0;
            this.Bid_Price = 0;
            this.Ask_Price = 0;
            this.Price_DateTime = new DateTime(627667488000000000, DateTimeKind.Unspecified);
        }
    }
}