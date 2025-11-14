using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Transactions;
using System.Data;
using System.Data.Common;
using BRPS.DAO;
using BRPS.Model;
using System.Data.Entity.Validation;
using EntityFramework.Mapping;
using System.Data.Entity.Infrastructure;

namespace BRPS.Repositories
{
    public abstract class RepositoryBase
    {
        public int InsertEntities<TEntity>(DbContext Context, List<TEntity> TEntitys, Int32 batchNos = 2000)  where TEntity: class
        {
            try
            {
                int records = TEntitys.Count;
                if (records == 0)
                {
                    return 0;
                }

                VerfifyContext(Context);
                int reminder = records % batchNos;
                int nos = records / batchNos + (reminder > 0 ? 1 : 0);
                records = 0;                
                for (int index = 0; index < nos; index++)
                {
                    Context.Set<TEntity>().AddRange(TEntitys.GetRange(index * batchNos, (index < nos - 1 || reminder == 0) ? batchNos : reminder));
                    records += SaveChanges(Context);
                }               
                return records;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.StackTrace);
                throw new Exception("Error in InsertEntities: " + ex.Message, ex.InnerException == null ? ex : ex.InnerException);
            }
        }

        public int ExecuteNonQuery(DbContext Context, string commandText, CommandType commandType = CommandType.Text, DbParameter[] parameters = null)
        {
            VerfifyContext(Context);
            var client = new GenericDAO(Context.Database.Connection);
            return client.ExecuteNonQuery(commandText, commandType, parameters); 
        }

        public DataSet ExecuteQuery(DbContext Context, string commandText, CommandType commandType = CommandType.Text, DbParameter[] parameters = null)
        {
            VerfifyContext(Context);
            var client = new GenericDAO(Context.Database.Connection);
            return client.ExecuteQuery(commandText, commandType, parameters);
        }

        #region protected method - SaveChanges
        protected int SaveChanges(DbContext Context)
        {
            try
            {
                VerfifyContext(Context);
                return Context.SaveChanges();
            }
            catch (Exception ex)
            {
                var Message = (ex.InnerException??ex).Message;
                var Message2 = Message;
                if (Context.GetValidationErrors() != null && Context.GetValidationErrors().Count()>0)
                {
                    Message = string.Format("There is(are) {0} validation error(s): ", Context.GetValidationErrors().Count());
                    Message2 = Message;
                    foreach (var err in Context.GetValidationErrors())
                    {
                        Message += (GetEntityKeyValue(Context, err.Entry) + ": \n");
                        foreach (DbValidationError err2 in err.ValidationErrors)
                        {
                            Message += string.Format("Property Name: {0}, Value: {1}, ErrorMessage: {2}\n" , err2.PropertyName, err.Entry.CurrentValues[err2.PropertyName], err2.ErrorMessage);
                            if (Message.Length <= 800)
                            {
                                Message2 = Message;
                            }
                        }
                    }
                }
                Console.WriteLine(Message);
                throw new Exception( Message2);
            }
        }

        protected static void VerfifyContext(DbContext Context)
        {
            if(Context==null)
            {
                throw new Exception("Error: Context hasn't been set.");
            }
        }

        private static string GetEntityKeyValue(DbContext Context, DbEntityEntry entity)
        {
            VerfifyContext(Context);
            var providor = new MetadataMappingProvider();
            var mapping = providor.GetEntityMap(entity.Entity.GetType(), Context);
            string keyValue =string.Format("Entity: {0} - ", mapping.EntityType.Name);
            foreach (var prop in mapping.KeyMaps)
            {
                keyValue += (string.Format("Column Name: {0}, Value: {1} ", prop.ColumnName, entity.CurrentValues[prop.ColumnName]));
            }
            return keyValue;
        }

        #endregion


        #region helper - GetPropertiesMapping
        public static string GetPropertiesMapping<TEntity>(DbContext Context)
        {
            VerfifyContext(Context);
            string smappings = "";           
            int colid = 0;
            var providor = new MetadataMappingProvider();
            var mapping = providor.GetEntityMap(typeof(TEntity), Context);
            foreach (var prop in mapping.PropertyMaps)
            {
                smappings += (string.Format("colid: {0} {1}: {2} \n", colid++, prop.ColumnName, prop.PropertyName));
            }
            Console.WriteLine(smappings);
            return smappings;
        }

        public static string GetTablename<TEntity>(DbContext Context)
        {
             var providor = new MetadataMappingProvider();
             var mapping = providor.GetEntityMap(typeof(TEntity),Context);
             return mapping.TableName;
        }

        #endregion
    }
}
