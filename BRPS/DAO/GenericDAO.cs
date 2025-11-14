using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Data.OleDb;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BRPS.DAO
{
    class GenericDAO : IDisposable
    {
        private bool _disposed=false;

        private IConnectionSettingsProvider _ConnectionSettingsProvider;
        private DbConnection _Connection;

        private ConnectionStringSettings _ConnectionStringSettings;

        public IConnectionSettingsProvider ConnectionSettingsProvider 
        { 
            get
            {
                return _ConnectionSettingsProvider;
            }
            set
            {
                _ConnectionSettingsProvider = value;
                _ConnectionStringSettings=value.GetConnectionStringSettings();
                Factory = DbProviderFactories.GetFactory(_ConnectionStringSettings.ProviderName);
            }
        }

        public DbConnection Connection
        {
            get
            {
                return _Connection;
            }
            set
            {
                _Connection = value;
                if (value != null)
                {
                    Factory = DbProviderFactories.GetFactory(_Connection);
                }
            }
        }

        public DbProviderFactory Factory { get; set; }
        
        protected GenericDAO()
        {

        }
        
        public GenericDAO(DbConnection connection)
        {
            Connection = connection;
            Factory = DbProviderFactories.GetFactory(connection);
        }


        public GenericDAO(IConnectionSettingsProvider connectionSettingsProvider)
        {
            this.ConnectionSettingsProvider = connectionSettingsProvider;            
        }

        public DbConnection OpenConnection()
        {
            if (Connection == null ) 
            {
                Connection = Factory.CreateConnection();
                Connection.ConnectionString = _ConnectionStringSettings.ConnectionString;
            }

            if(Connection.State != ConnectionState.Open)
            {
                Connection.Open();
            }
            return Connection;
        }

        public DbDataReader ExecuteReader(string commandText, CommandType commandType = CommandType.Text, DbParameter[] parameters = null)
        {
            OpenConnection();
            using (var cmd = Connection.CreateCommand())
            {
                cmd.CommandText = commandText;
                cmd.CommandType = commandType;
                if (parameters != null)
                {
                    cmd.Parameters.AddRange(parameters);
                }
                return cmd.ExecuteReader();
            }
            
        }

        public int ExecuteNonQuery(string commandText, CommandType commandType = CommandType.Text, DbParameter[] parameters = null)
        {
            OpenConnection();
            using (var cmd = Connection.CreateCommand())
            {
                cmd.CommandText = commandText;
                cmd.CommandType = commandType;
                if (parameters != null)
                {
                    cmd.Parameters.AddRange(parameters);
                }
                return cmd.ExecuteNonQuery();
            }
        }

        public DataSet ExecuteQuery(string commandText, CommandType commandType = CommandType.Text, DbParameter[] parameters=null)
        {  
            OpenConnection();
            using (var cmd = Connection.CreateCommand())
            {
                cmd.CommandTimeout = 3600000;
                cmd.CommandText = commandText;
                cmd.CommandType = commandType;
                if (parameters != null)
                {
                    cmd.Parameters.AddRange(parameters);
                }

                var adapter =Factory.CreateDataAdapter();
                adapter.SelectCommand = cmd;
                var ds = new DataSet();

                adapter.Fill(ds);
                return ds;
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (_disposed)
            {
                return;
            }

            CloseConnection();

            if (disposing)
            {
                Connection.Dispose();
                Connection = null;
            }
            _disposed = true;
        }

        public void CloseConnection()
        {
            if (Connection != null && Connection.State != ConnectionState.Closed)
            {
                Connection.Close();
            }
        }

        ~GenericDAO()
        {
            Dispose(false);
        }
    }

}
