using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BRPS.Model
{
    public partial class Holiday
    {
        public Holiday()
        {
            Initialize();
        }

        public void Initialize()
        {
            this.Holiday_Id = "";
            this.Holiday_Date = new DateTime(627667488000000000, DateTimeKind.Unspecified);
            this.Weekend_Day = 0;
        }
    }
}