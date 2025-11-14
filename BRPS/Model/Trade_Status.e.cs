using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BRPS.Model
{
    public partial class Trade_Status
    {
        public Trade_Status()
        {
            Initialize();
        }

        public void Initialize()
        {
            Settle_Status = "N";
        }
    }
}
