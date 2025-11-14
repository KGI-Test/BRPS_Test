using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BRPS.Model
{
    public partial class Instrument_Price
    {
        public Instrument_Price()
        {
            Initialize();
        }

        public void Initialize()
        {
            this.Instr_Ref = "";
            this.Close_Price = 0;
            this.Price_date = new DateTime(627667488000000000, DateTimeKind.Unspecified);
            this.Version_date = new DateTime(627667488000000000, DateTimeKind.Unspecified);
            this.Version_no = 0;
        }
    }
}
