using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BRPS.Managers
{
    public abstract class DataManagerBase<TEntity> : DataManagerBase
        where TEntity : class, new()
    {
        public List<TEntity> LoadEntitiesFromFile(String filename)
        {
            return LoadEntitiesFromFile<TEntity>(filename);
        }
  
        protected override object ParseObjectData(Type type,string data) 
        {
            return this.ParseData(data);
        }

        protected abstract TEntity ParseData(string data);
    }



    public abstract class DataManagerBase 
    {
        public String Message { get; set; }

        public List<TEntity> LoadEntitiesFromFile<TEntity>(String filename) where TEntity:class , new()
        {
            Message = "";
            var entities = new List<TEntity>();
            try
            {
                using (var reader = File.OpenText(filename))
                {
                    while (!reader.EndOfStream)
                    {
                        string data = reader.ReadLine();

                        var entity = ParseObjectData(typeof(TEntity), data);                        
                        if (entity != null && entity is TEntity)
                        {
                            entities.Add((TEntity)entity);
                        }                        
                    }
                }
            }
            catch (Exception ex)
            {
                Message = ex.Message;
                Console.WriteLine(ex.Message);
                return null;
            }

            return entities;
        }

        protected virtual bool VerifyData(ref string data, int length, bool detailInd = true)
        {
            if (data.Length == 0)
            {
                return false;
            }

            String recordType = data.Substring(0, 1);
            if (detailInd)
            {
                if (recordType != "D")
                {
                    return false;
                }
            }

            if (data.Length < length)
            {
                data = data + new String(' ', length - data.Length);
            }

            return true;
        }

        protected virtual object ParseObjectData(Type type, string data)
        {
            throw new NotImplementedException();
        }
    }
}
