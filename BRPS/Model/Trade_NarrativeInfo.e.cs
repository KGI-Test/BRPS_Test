using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BRPS.Model
{
    public partial class Trade_NarrativeInfo
    {
        public Trade_NarrativeInfo()
        {
            Initialize();
        }

        public void Initialize()
        {
            this.Narrative_Code = "";
            this.Narrative = "";
            this.Version_date = new DateTime(627667488000000000, DateTimeKind.Unspecified);
            this.Version_no = 0;
        }
    }
}
