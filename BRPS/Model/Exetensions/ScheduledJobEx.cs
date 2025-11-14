using BRPS.Model;
using GalaSoft.MvvmLight;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BRPS.Model.Extensions
{
    public class ScheduledJobEx : ObservableObject , IDataErrorInfo
    {
        public ScheduledJobEx(ScheduledJob job)
        {
            Job = job;            
        }

     
        private bool _selected = false;

        private ScheduledJob _job = null;

        public ScheduledJob Job
        {
            get { return _job; }
            set
            {
                _job = value;
                ScheduledJobParams = new ObservableCollection<ScheduledJobParam>(_job.ScheduledJobParams.OrderBy(j=>j.Param_Order));
                RaisePropertyChanged("Job");
            }
        }

        public bool Selected
        {
            get
            {
                return _selected;
            }
            set
            {
                if (_selected != value)
                {
                    _selected = value;
                    RaisePropertyChanged("Selected");
                }
            }
        }

        public DateTime? Last_Execution_Time
        {
            get
            {
                return Job.Last_Execution_Time?? new DateTime(1990,1,1);
            }
            set
            {
                Job.Last_Execution_Time = value;
                RaisePropertyChanged("Last_Execution_Time");
            }
        }

        public int Status
        {
            get
            {
                return Job.Status;
            }
            set
            {
                Job.Status = value;  
                RaisePropertyChanged("Status");
            }
        }

        public DateTime Business_Date
        {
            get { return Job.Business_Date; }
            set
            {
                Job.Business_Date = value;
                RaisePropertyChanged("Business_Date");
            }
        }

    
        public string Message
        {
            get { return Job.Message; }
            set { 
                Job.Message= value;
                RaisePropertyChanged("Message");
            }
        }

        /// <summary>
        /// The ScheduledJobParams in entity ScheduledJob is IConnection which is not updatable in DataGrid. Change it to IList so that it can be editable.
        /// </summary>
        private ObservableCollection<ScheduledJobParam> _scheduledJobParams;
        public ObservableCollection<ScheduledJobParam> ScheduledJobParams
        {
            get { return _scheduledJobParams; }
            set { _scheduledJobParams = value;
                RaisePropertyChanged("ScheduledJobParams");
            }
        }

        public void UpdateScheduledJobParams()
        {
            Job.ScheduledJobParams.Clear();
            foreach (var para in ScheduledJobParams)
	        {
		        Job.ScheduledJobParams.Add(para);
	        }
        }

        public static string[] Occurences
        {
            get
            {
                return new string[] { "Interval","Daily", "Weekday", "Weekend", "Monthend" };
            }
        }

        public static string[] ParameterNames
        {
            get
            {
                return new string[] { "BUSINESS_DATE", "DATE", "LOCAL_SRC_FILE_NAME", "LOCAL_SRC_FILE_PATH", "REMOTE_SRC_FILE_NAME",
                        "REMOTE_SRC_FILE_PATH","APP_SETTING","OTHER"};
            }
        }

        public static string[] ParameterTypes
        {
            get
            {
                var qry = from s in Enum.GetNames(typeof(TypeCode))
                          where !s.Equals("Empty")
                          select "System." + s;
                return qry.ToArray();
            }
        }

        private Dictionary<string,string> _errors=new Dictionary<string,string>();        

        public Dictionary<string,string> Errors
        {
            get { return _errors; }
            set { _errors = value; }
        }
        
        public string Error
        {
            get {
                return this[String.Empty];
            }
        }

        public string this[string columnName]
        {
            get 
            { 
                string err="";
                if(string.IsNullOrEmpty(columnName) || columnName=="Description")
                {
                    if(string.IsNullOrEmpty(Job.Description))
                    {
                        err = "The description cannot be null or empty.";
                        _errors.Add("Description", err);
                        return err;
                    }
                }
                else if (columnName == "From_Time")
                {
                    DateTime dt;
                    if (DateTime.TryParse("1990-01-01 " + Job.From_Time, out dt))
                    {
                        err = "The value of From_Time is invalid.";
                        _errors.Add("From Time", err);
                        return err;
                    }
                }
                else if (columnName == "End_Time")
                {
                    DateTime dt;
                    if (DateTime.TryParse("1990-01-01 " + Job.End_Time, out dt))
                    {
                        err = "The value of End_Time is invalid.";
                        _errors.Add("End Time", err);
                        return err;
                    }
                }
                return "";
            }
        }
    }


    
}
