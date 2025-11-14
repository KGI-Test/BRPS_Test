using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Data.Entity.Core.Objects;
using System.Data.Entity.Infrastructure;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BRPS.Model
{
    public partial class BRPSDbContext
    {
        [DbFunction("BRPSModel.Store", "GetBusinessDate")]
        public DateTime GetBusinessDate(string cutoffTime)
        {
            var objectContext = ((IObjectContextAdapter)this).ObjectContext;

            return objectContext.CreateQuery<DateTime>("BRPSModel.Store.GetBusinessDate(@CutoffTime)", new ObjectParameter[] { new ObjectParameter("CutoffTime", cutoffTime) })
                .Execute(MergeOption.NoTracking).FirstOrDefault();

        }
    }
}
