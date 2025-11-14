using BRPS.Model;
using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BRPS.Repositories
{
    public interface IScheduledJobRepository
    {
        int DeleteScheduledJob(ScheduledJob job);
        int EditScheduledJob(ScheduledJob editScheduledJob);
        List<ScheduledJob> GetAllScheduledJobs();
        DateTime GetBusinessDate(string cutoffTime);
        ScheduledJob GetScheduledJobByJobId(int Job_id);
        int InsertScheduledJob(ScheduledJob job);
    }

    class ScheduledJobRepository : RepositoryBase, IScheduledJobRepository
    {        
        #region Deconstructure
        ~ScheduledJobRepository()
        {
            try
            {
                ;
            }
            catch (Exception)
            {
                ;
            }
        }
        #endregion

        public List<ScheduledJob> GetAllScheduledJobs()
        {
            using (var context = new BRPSDbContext())
            {
                return context.ScheduledJobs.Include(j => j.ScheduledJobParams).OrderBy(job => job.Interval).ThenBy(job => job.Job_Id).ToList();
            }
        }

        public ScheduledJob GetScheduledJobByJobId(int Job_id)
        {
            using (var context = new BRPSDbContext())
            {
                return context.ScheduledJobs.Include(j => j.ScheduledJobParams).Where(j => j.Job_Id == Job_id).FirstOrDefault();  
            }           
        }

        public int DeleteScheduledJob(ScheduledJob job)
        {
            using (var context = new BRPSDbContext())
            {
                context.ScheduledJobs.Remove(job);
                return SaveChanges(context);
            }
        }

        public int InsertScheduledJob(ScheduledJob job)
        {
            using (var context = new BRPSDbContext())
            {
                context.ScheduledJobs.Add(job);
                return SaveChanges(context);
            }
        }

        public int EditScheduledJob(ScheduledJob editScheduledJob)
        {
            using (var context = new BRPSDbContext())
            {
                var dbJob = context.ScheduledJobs.Include(j => j.ScheduledJobParams).Where(j => j.Job_Id == editScheduledJob.Job_Id).FirstOrDefault();
                var entry = context.Entry<ScheduledJob>(dbJob);
                entry.CurrentValues.SetValues(editScheduledJob);
                entry.State = EntityState.Modified;

                foreach (var editParam in editScheduledJob.ScheduledJobParams)
                {
                    var dbParam = dbJob.ScheduledJobParams.FirstOrDefault(para => para.Param_Id == editParam.Param_Id);
                    if (dbParam != null)
                    {
                        var dbParaEntry = context.Entry(dbParam);
                        dbParaEntry.CurrentValues.SetValues(editParam);
                        dbParaEntry.State = EntityState.Modified;
                    }
                    else
                    {
                        dbJob.ScheduledJobParams.Add(editParam);
                        editParam.ScheduledJob = dbJob;
                    }
                }

                var deletedParams = new List<ScheduledJobParam>();
                foreach (var dbParam in dbJob.ScheduledJobParams)
                {
                    var editParam = editScheduledJob.ScheduledJobParams.FirstOrDefault(para => para.Param_Id == dbParam.Param_Id);
                    if (editParam == null)
                    {
                        deletedParams.Add(dbParam);
                    }
                }

                foreach (var delParam in deletedParams)
                {
                    context.ScheduledJobParams.Remove(delParam);
                    dbJob.ScheduledJobParams.Remove(delParam);
                }
                return SaveChanges(context);
            }
        }


        public DateTime GetBusinessDate(string cutoffTime)
        {
            using (var context = new BRPSDbContext())
            {
                return context.GetBusinessDate(cutoffTime);
            }
        }
    }
}
