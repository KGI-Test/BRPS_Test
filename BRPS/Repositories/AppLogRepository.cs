using BRPS.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BRPS.Repositories
{
    public interface IAppLogRepository
    {
        int DeleteAppLog(AppLog log);
        List<AppLog> GetAppLogs();
        List<AppLog> GetAppLogsByNum(int Num, DateTime startDate);
        List<AppLog> GetNewAppLogs(int lastLogId);
        int InsertAppLog(AppLog log);
    }

    class AppLogRepository : RepositoryBase, IAppLogRepository
    {   
        private BRPSDbContext Context=new BRPSDbContext();

        ~AppLogRepository()
        {
            try
            {
                Context.Dispose();
            }
            catch (Exception)
            {
                ;
            }
        }

        public int InsertAppLog(AppLog log)
        {        
             Context.AppLogs.Add(log);
             return SaveChanges(Context);
        }

        public int DeleteAppLog(AppLog log)
        {            
            Context.AppLogs.Remove(log);
            return SaveChanges(Context);            
        }

        public List<AppLog> GetAppLogs()
        {            
            return Context.AppLogs.OrderByDescending(log => log.Log_Id).ToList();
        }

        public List<AppLog> GetAppLogsByNum(int Num, DateTime startDate)
        {
            return Context.AppLogs.Where(log => log.Log_Time.CompareTo(startDate) >= 0).OrderBy(log => log.Log_Id).Take(Num).ToList();
        }

        public List<AppLog> GetNewAppLogs(int lastLogId)
        {
            return Context.AppLogs.Where( log=> log.Log_Id>lastLogId ).OrderBy(log => log.Log_Id).Take(100).ToList();
        }
    }
}
