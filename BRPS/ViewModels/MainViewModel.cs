using BRPS.Managers;
using BRPS.Model;
using BRPS.Model.Extensions;
using BRPS.Views;
using BRPS.Repositories;
using GalaSoft.MvvmLight;
using GalaSoft.MvvmLight.Command;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.IO;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Input;
using System.Windows.Threading;
using System.Windows.Controls.Primitives;
using GalaSoft.MvvmLight.Messaging;
using System.Configuration;


namespace BRPS.ViewModels
{
    /// <summary>
    /// This class contains properties that the main View can data bind to.
    /// </summary>
    public class MainViewModel : ViewModelBase
    {        
        private readonly string SelectAllButtonContent="Select All" ;
        private readonly string DeselectAllButtonContent="Deselect All" ;

        private readonly string AutoRunButtonContextOFF = "Auto Run (OFF)";
        private readonly string AutoRunButtonContextON = "Auto Run (ON)";

        private DispatcherTimer autoJobTimer = null;
       
        private IScheduledJobRepository _jobsRepository; 
        private IAppLogRepository _appLogRepository;
        private IScheduledJobService _scheduledJobService;
        private readonly bool AutoRunSetting = Convert.ToBoolean(ConfigurationManager.AppSettings["AutoRun"]);

        private readonly bool RunLateBooking = Convert.ToBoolean(ConfigurationManager.AppSettings["RunLateBooking"]);
        private readonly string LateBookingFile = string.Format(ConfigurationManager.AppSettings["LateBookingFile"],DateTime.Now);

        private Window _view;
        
        KGILogger m_logger = null;

        /// <summary>
        /// Initializes a new instance of the MainViewModel class.
        /// </summary>
        public MainViewModel(IScheduledJobRepository jobsRepository, IAppLogRepository appLogRepository,
            IScheduledJobService scheduledJobService)
        {
            _jobsRepository = jobsRepository;
            _appLogRepository=appLogRepository;
            _scheduledJobService = scheduledJobService;

            this.initLog();
        }

        private ObservableCollection<AppLog> _appLogs;
        private List<ScheduledJobEx> _scheduledJobs;

        private ICollectionView _jobCollectionView = null;
        private ICollectionView _logCollectionView = null;

        public ICollectionView ScheduledJobCV
        {
            get { return _jobCollectionView; }
            set
            {
                _jobCollectionView = value;
                RaisePropertyChanged("ScheduledJobCV");
            }
        }

        public ICollectionView AppLogCV
        {
            get { return _logCollectionView; }
            set
            {
                _logCollectionView = value;
                RaisePropertyChanged("AppLogCV");
            }
        }

        public void LoadScheduledJobs()
        {
            try
            {           
                _scheduledJobs = new List<ScheduledJobEx>();
                var jobs = _jobsRepository.GetAllScheduledJobs();
                foreach (var job in jobs)
                {
                    _scheduledJobs.Add(new ScheduledJobEx(job));
                }

                ScheduledJobCV = CollectionViewSource.GetDefaultView(_scheduledJobs);
                ScheduledJobCV.GroupDescriptions.Add(new PropertyGroupDescription("Job.Occurence"));
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        public void LoadAppLogs()
        {
            try
            {                
                var logs = _appLogRepository.GetAppLogsByNum(100, DateTime.Now.Date);
                _appLogs = new ObservableCollection<AppLog>(logs);

                this.initLog();

                AppLogCV = CollectionViewSource.GetDefaultView(_appLogs);
                ScrollLastLogIntoView();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        public void initLog()
        {
            try
            { 
                if (this.m_logger == null)
                {
                    String strLogName = "";

                    this.m_logger = new KGILogger(strLogName);
                }


                m_logger.TurnOnDebugScreen = false;

                m_logger.Info("------ " + "BRPS" + " started ------");
            }
            catch (Exception Ex)
            {
                if (this.m_logger != null)
                {
                    this.m_logger.Error("initLog - Error: " + Ex.Message);
                }
            }
        }

        private DateTime _businessDate;

        public DateTime BusinessDate
        {
            get { return _businessDate; }
            set {
                //_businessDate = value; Comment Out By : Jay on 13 Jan 2017
                _businessDate = DateTime.Now.AddDays(-1);
                RaisePropertyChanged("BusinessDate");
            }
        }

        public void UpdateBusinessdate(string cutoffTime = "06:00")
        {
            BusinessDate = _jobsRepository.GetBusinessDate(cutoffTime);
        }

        private RelayCommand<object> _onloadedCommand;

        /// <summary>
        /// Gets the OnloadCommand.
        /// </summary>
        public RelayCommand<object> OnWindowLoadedCommand
        {
            get
            {
                return _onloadedCommand ?? (_onloadedCommand = new RelayCommand<object>(Window_Loaded));
            }
        }

        private void Window_Loaded(object obj)
        {
            try
            {
                _view = (Window)obj;
                UpdateBusinessdate();
                LoadAppLogs();
                LoadScheduledJobs();

                initLog();

                if (AutoRunSetting)
                {
                    Run_AutoMode();
                }

                // for late booking
                if(RunLateBooking)
                {
                    //
                    //LateBookingFile
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine((ex.InnerException??ex).Message);
            }
             
        }
    
        private RelayCommand<Button> _selectAllCommand;

        private void Run_AutoMode()
        {
            var curTime = DateTime.Now.ToString("HH:mm");

            if (m_logger == null)
            {
                initLog();
            } 
            
           if ( curTime.CompareTo("16:00") > 0 && curTime.CompareTo("24:00") < 0 ) // Download Client,Instrument & Salesperson
            {
                if (!IsAutoMode)
                {
                    autoJobTimer = new DispatcherTimer(DispatcherPriority.Background);
                    autoJobTimer.Tick += new EventHandler(OnAutoRunTimer_Tick);
                    autoJobTimer.Interval = new TimeSpan(0, 0, 30);
                    autoJobTimer.Start();
                    IsAutoMode = true;
                }
                else
                {
                    if (autoJobTimer != null)
                    {
                        autoJobTimer.Stop();
                        IsAutoMode = false;
                    }
                }
            }
            else if( curTime.CompareTo("00:00") > 0 && curTime.CompareTo("04:00") < 0 ) //EOD Trade Download
            //else if( curTime.CompareTo("18:00") > 0 && curTime.CompareTo("24:00") < 0 ) //EOD Trade Download
            {
                if (!IsAutoMode)
                {
                    autoJobTimer = new DispatcherTimer(DispatcherPriority.Background);
                    autoJobTimer.Tick += new EventHandler(OnAutoRunTimer_Tick1);
                    autoJobTimer.Interval = new TimeSpan(0, 0, 30);
                    autoJobTimer.Start();
                    IsAutoMode = true;
                }
                else
                {
                    if (autoJobTimer != null)
                    {
                        autoJobTimer.Stop();
                        IsAutoMode = false;
                    }
                }
            }
            else if (curTime.CompareTo("05:00") > 0 && curTime.CompareTo("11:00") < 0) // US Trade Download
            {
                if (!IsAutoMode)
                {
                    autoJobTimer = new DispatcherTimer(DispatcherPriority.Background);
                    autoJobTimer.Tick += new EventHandler(OnAutoRunTimer_Tick2);
                    autoJobTimer.Interval = new TimeSpan(0, 0, 30);
                    autoJobTimer.Start();
                    IsAutoMode = true;
                }
                else
                {
                    if (autoJobTimer != null)
                    {
                        autoJobTimer.Stop();
                        IsAutoMode = false;
                    }
                }
            }
           else if (curTime.CompareTo("12:00") > 0 && curTime.CompareTo("23:00") < 0) // Central Dealer Trades
           {
               if (!IsAutoMode)
               {
                   autoJobTimer = new DispatcherTimer(DispatcherPriority.Background);
                   autoJobTimer.Tick += new EventHandler(OnAutoRunTimer_Tick4);
                   autoJobTimer.Interval = new TimeSpan(0, 0, 30);
                   autoJobTimer.Start();
                   IsAutoMode = true;
               }
               else
               {
                   if (autoJobTimer != null)
                   {
                       autoJobTimer.Stop();
                       IsAutoMode = false;
                   }
               }
           }
        }

        /// <summary>
        /// Gets the SelectAllCommand
        /// </summary>
        public RelayCommand<Button> SelectAllCommand
        {
            get
            {
                return _selectAllCommand ?? (_selectAllCommand = new RelayCommand<Button>(OnSelectAllCommand));
            }
        }

        private RelayCommand<object> _excuteCommand;

        /// <summary>
        /// Gets the ExecuteCommand.
        /// </summary>
        public RelayCommand<object> ExecuteCommand
        {
            get
            {
                return _excuteCommand ?? (_excuteCommand = new RelayCommand<object>(OnExecuteCommand));
            }
        }

        private void OnExecuteCommand(object obj)
        {
            OnExecuteCommand(obj, false);
        }

        private void OnExecuteCommand(object obj, bool AutoMode) // EOD Static Data
        {            
            try
            {
                var businessDate = (DateTime)obj;
                foreach (var jobModel in _scheduledJobs)
                {                    
                    var job = jobModel.Job;
                    var curTime = DateTime.Now.ToString("HH:mm");


                    if (jobModel.Job.Process == "LoadTrades_EOD_CAGS")
                    {
                        jobModel.Selected = true;
                    }

                    try
                    {
                        var isDone = jobModel.Business_Date.Equals(BusinessDate) && (jobModel.Status == 1);
                        if ( (!isDone && AutoMode))
                        {
                            _view.Cursor = Cursors.Wait;

                            if (job.Description == "Load Instruments Price" || job.Description == "Load Instruments" || job.Description == "Load Salepersons" || job.Description == "Load Clients")
                            {
                                AddAppLog("INFO", job.Description, "Starting to execute the job ...");

                                _scheduledJobService.Execute(jobModel, businessDate, AutoMode, m_logger);
                                _jobsRepository.EditScheduledJob(job);

                                AddAppLog(job.Status == 2 ? "ERR" : "INFO", job.Description, "The job has been completed - " + job.Message);

                                if ((job.Description == "Load Instruments Price") && job.Status == 1)
                                {
                                    Environment.Exit(0);
                                }
                            }
                        }
                        else if(jobModel.Selected)
                        {
                            _view.Cursor = Cursors.Wait;

                                AddAppLog("INFO", job.Description, "Starting to execute the job ...");

                                _scheduledJobService.Execute(jobModel, businessDate, AutoMode, m_logger);
                                _jobsRepository.EditScheduledJob(job);

                                AddAppLog(job.Status == 2 ? "ERR" : "INFO", job.Description, "The job has been completed - " + job.Message);    
                        }

                    }
                    catch (Exception ex)
                    {
                        job.Message = "ERROR: " + ex.Message;
                        AddAppLog("ERR", job.Description, (ex.InnerException ?? ex).Message);
                    }                    
                }
            }
            finally
            {
                _view.Cursor = Cursors.Arrow;
            }
        }

        private void OnExecuteCommand1(object obj, bool AutoMode) // EOD Trades
        {
            try
            {
                var businessDate = (DateTime)obj;
                foreach (var jobModel in _scheduledJobs)
                {
                    var job = jobModel.Job;
                    var curTime = DateTime.Now.ToString("HH:mm");

                    try
                    {
                        var isDone = jobModel.Business_Date.Equals(BusinessDate) && (jobModel.Status == 1);
                        if ((!isDone && AutoMode) || jobModel.Selected)
                        {
                            _view.Cursor = Cursors.Wait;

                            if (job.Description == "Load Stock Balance" || job.Description == "Load Cash Balance" || job.Description == "Load Trade Narrative" ||
                                job.Description == "Load Trade Narrative" || job.Description == "Load Amended Trade" || job.Description == "Load Cancelled Trade" ||
                                job.Description == "Load Trades - EOD - BAGS" || job.Description == "Load Trades - EOD - CCAC" || job.Description == "Load Trades - EOD - SCCF" ||
                                job.Description == "Load Trades - EOD - CAGS" || job.Description == "Load Trades - EOD - FRES" || job.Description == "Load Trades - EOD - FREC - OPEN" || job.Description == "Load Trades - EOD - FREC - SETTLED")
                            {
                                AddAppLog("INFO", job.Description, "Starting to execute the job ...");

                                //m_logger.Info(job.Description);

                                _scheduledJobService.Execute(jobModel, businessDate, AutoMode, m_logger); //ryan
                                _jobsRepository.EditScheduledJob(job);

                                AddAppLog(job.Status == 2 ? "ERR" : "INFO", job.Description, "The job has been completed - " + job.Message);

                                if ((job.Description == "Load Trades - EOD - CAGS") && job.Status == 1)
                                {
                                    Environment.Exit(0);
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        job.Message = "ERROR: " + ex.Message;
                        AddAppLog("ERR", job.Description, (ex.InnerException ?? ex).Message);
                    }
                }
            }
            finally
            {
                _view.Cursor = Cursors.Arrow;
            }
        }

        private void OnExecuteCommand2(object obj, bool AutoMode) // US Trades
        {
            try
            {
                var businessDate = (DateTime)obj;
                foreach (var jobModel in _scheduledJobs)
                {
                    var job = jobModel.Job;
                    var curTime = DateTime.Now.ToString("HH:mm");

                    try
                    {
                        var isDone = jobModel.Business_Date.Equals(BusinessDate) && (jobModel.Status == 1);

                        if ((!isDone && AutoMode) || jobModel.Selected)
                        {
                            _view.Cursor = Cursors.Wait;

                            if (job.Description == "Load Trades - EOD - USTRADE")
                            {
                                AddAppLog("INFO", job.Description, "Starting to execute the US Trade job ...");

                                _scheduledJobService.Execute(jobModel, businessDate, AutoMode, m_logger);
                                _jobsRepository.EditScheduledJob(job);
                                AddAppLog(job.Status == 2 ? "ERR" : "INFO", job.Description, "The job has been completed - " + job.Message);
                            }

                            if (job.Description == "Load Trades - EOD - USTRADE" && job.Status == 1)
                            {
                                Environment.Exit(0);
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        job.Message = "ERROR: " + ex.Message;
                        AddAppLog("ERR", job.Description, (ex.InnerException ?? ex).Message);
                    }
                }
            }
            finally
            {
                _view.Cursor = Cursors.Arrow;
            }
        }

        private void OnExecuteCommand3(object obj, bool AutoMode)
        {
            try
            {
                var businessDate = (DateTime)obj;
                foreach (var jobModel in _scheduledJobs)
                {
                    var job = jobModel.Job;
                    var curTime = DateTime.Now.ToString("HH:mm");

                    try
                    {
                        var isDone = jobModel.Business_Date.Equals(BusinessDate) && (jobModel.Status == 1);

                        if ((!isDone && AutoMode) || jobModel.Selected)
                        {
                            _view.Cursor = Cursors.Wait;

                            if (job.Description == "Load Clients - EOD - 6PM")
                            {
                                AddAppLog("INFO", job.Description, "Starting to execute the job ...");

                                _scheduledJobService.Execute(jobModel, businessDate, AutoMode, m_logger);
                                _jobsRepository.EditScheduledJob(job);
                                AddAppLog(job.Status == 2 ? "ERR" : "INFO", job.Description, "The job has been completed - " + job.Message);
                            }

                            if (job.Description == "Load Clients - EOD - 6PM" && job.Status == 1)
                            {
                                Environment.Exit(0);
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        job.Message = "ERROR: " + ex.Message;
                        AddAppLog("ERR", job.Description, (ex.InnerException ?? ex).Message);
                    }
                }
            }
            finally
            {
                _view.Cursor = Cursors.Arrow;
            }
        }

        private void OnExecuteCommand4(object obj, bool AutoMode) // Central Dealer Trades
        {
            try
            {
                var businessDate = (DateTime)obj;
                foreach (var jobModel in _scheduledJobs)
                {
                    var job = jobModel.Job;
                    var curTime = DateTime.Now.ToString("HH:mm");

                    try
                    {
                        var isDone = jobModel.Business_Date.Equals(BusinessDate) && (jobModel.Status == 1);

                        if ((!isDone && AutoMode) || jobModel.Selected)
                        {
                            _view.Cursor = Cursors.Wait;

                            if (job.Description == "Load Trades - EOD - CDEALER")
                            {
                                AddAppLog("INFO", job.Description, "Starting to execute the CDEALER Trade job ...");

                                _scheduledJobService.Execute(jobModel, businessDate, AutoMode, m_logger);
                                _jobsRepository.EditScheduledJob(job);
                                AddAppLog(job.Status == 2 ? "ERR" : "INFO", job.Description, "The job has been completed - " + job.Message);
                            }

                            if (job.Description == "Load Trades - EOD - CDEALER" && job.Status == 1)
                            {
                                Environment.Exit(0);
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        job.Message = "ERROR: " + ex.Message;
                        AddAppLog("ERR", job.Description, (ex.InnerException ?? ex).Message);
                    }
                }
            }
            finally
            {
                _view.Cursor = Cursors.Arrow;
            }
        }

        private void OnSelectAllCommand(Button btn)
        {
            try
            {
                if (btn == null)
                {
                    return;
                }
                bool selectAll = (btn.Content.ToString() == SelectAllButtonContent);
                foreach (var job in _scheduledJobs)
                {
                    job.Selected = selectAll;
                }

                btn.Content = selectAll ? DeselectAllButtonContent : SelectAllButtonContent;    
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            } 
        }

        private RelayCommand<object> _EditJobCommand;

        /// <summary>
        /// Gets the EditJobCommand.
        /// </summary>
        public RelayCommand<object> EditJobCommand
        {
            get
            {
                return _EditJobCommand ?? (_EditJobCommand = new RelayCommand<object>(OnEditJobCommand));
            }
        }

        private void OnEditJobCommand(object obj)
        {
            try
            {
                var job =(ScheduledJobEx)_jobCollectionView.CurrentItem;
                if (job != null)
                {
                    var window = new EditScheduledJobWindow();
                    Messenger.Default.Send<EditScheduledJobMessage>(new EditScheduledJobMessage(job));
                    window.ShowDialog();
                    LoadScheduledJobs();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }

        }

        private RelayCommand _AddJobCommand;

        /// <summary>
        /// Gets the AddJobCommand.
        /// </summary>
        public RelayCommand AddJobCommand
        {
            get
            {
                return _AddJobCommand ?? (_AddJobCommand = new RelayCommand(OnAddJobCommand));
            }
        }

        private void OnAddJobCommand()
        {
            try
            {
                var window = new EditScheduledJobWindow();
                Messenger.Default.Send<EditScheduledJobMessage>(new EditScheduledJobMessage(null));
                window.ShowDialog();
                LoadScheduledJobs();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }

        }

        private RelayCommand _deleteJobCommand;

        /// <summary>
        /// Gets the DeleteJobCommand.
        /// </summary>
        public RelayCommand DeleteJobCommand
        {
            get
            {
                return _deleteJobCommand ?? (_deleteJobCommand = new RelayCommand(OnDeleteJobCoomand));
            }
        }

        private void OnDeleteJobCoomand()
        {
            var jobEx = (ScheduledJobEx)_jobCollectionView.CurrentItem;
            try 
	        {
                if (jobEx == null || MessageBox.Show("Are you sure to delete the scheduled job " + jobEx.Job.Description + "?", "Scheduled Job Deletion",
                        MessageBoxButton.YesNo, MessageBoxImage.Question) == MessageBoxResult.No)
                {
                    return;
                }

                _jobsRepository.DeleteScheduledJob(jobEx.Job);
                LoadScheduledJobs();
                MessageBox.Show("Sucessfully delete the scheduled job " + jobEx.Job.Description + "!", "Scheduled Job Deletion",
                        MessageBoxButton.OK, MessageBoxImage.Information);
   
	        }
	        catch (Exception ex)
	        {
                MessageBox.Show("Failure to delete the scheduled job " + jobEx.Job.Description + ". Cause: " + (ex.InnerException ?? ex).Message, 
                        "Scheduled Job Deletion",  MessageBoxButton.OK, MessageBoxImage.Error);
	        }
            
        }

        private bool CanDeleteJob()
        {
            var jobEx = (ScheduledJobEx)_jobCollectionView.CurrentItem;
            if (jobEx == null)
            {
                return false;
            }

            return MessageBox.Show("Are you sure to delete the scheduled job " + jobEx.Job.Description + "?", "Scheduled Job Deletion",
                MessageBoxButton.YesNo,MessageBoxImage.Question)==MessageBoxResult.Yes;
        }

        private RelayCommand<object> _autoRunCommand;

        /// <summary>
        /// Gets the AutoRunCommand.
        /// </summary>
        public RelayCommand<object> AutoRunCommand
        {
            get
            {
                return _autoRunCommand ?? (_autoRunCommand = new RelayCommand<object>(OnAutoRunCommand));
            }
        }

        private void OnAutoRunCommand(object obj)
        {
            var btn = obj as ToggleButton;
            if (!IsAutoMode)
            {
                autoJobTimer = new DispatcherTimer(DispatcherPriority.Background);
                autoJobTimer.Tick += new EventHandler(OnAutoRunTimer_Tick);
                autoJobTimer.Interval = new TimeSpan(0, 1, 0);
                autoJobTimer.Start();
                if (btn != null)
                {
                    btn.Content = AutoRunButtonContextON;
                }
                IsAutoMode = true;
            }
            else
            {
                if (autoJobTimer != null)
                {
                    autoJobTimer.Stop();
                    IsAutoMode = false;
                }
                if (btn != null)
                {
                    btn.Content = AutoRunButtonContextOFF;
                }
            }
        }

        private void OnAutoRunCommand()
        {
            if (!IsAutoMode)
            {
                autoJobTimer = new DispatcherTimer(DispatcherPriority.Background);
                autoJobTimer.Tick += new EventHandler(OnAutoRunTimer_Tick);
                autoJobTimer.Interval = new TimeSpan(0, 1, 0);
                autoJobTimer.Start();

                IsAutoMode = true;
            }
            else
            {
                if (autoJobTimer != null)
                {
                    autoJobTimer.Stop();
                    IsAutoMode = false;
                }
            }
        }

        private void OnAutoRunTimer_Tick(object sender, EventArgs e)
        {
            try
            {
                BusinessDate = _jobsRepository.GetBusinessDate("08:00");
                OnExecuteCommand(BusinessDate, true);
            }
            catch (Exception ex)
            {
                Console.WriteLine((ex.InnerException ?? ex).Message);                
            }
        }

        private void OnAutoRunTimer_Tick1(object sender, EventArgs e)
        {
            try
            {
                BusinessDate = _jobsRepository.GetBusinessDate("08:00");
                OnExecuteCommand1(BusinessDate, true);
            }
            catch (Exception ex)
            {
                Console.WriteLine((ex.InnerException ?? ex).Message);
            }
        }

        private void OnAutoRunTimer_Tick2(object sender, EventArgs e)
        {
            try
            {
                BusinessDate = _jobsRepository.GetBusinessDate("08:00");
                OnExecuteCommand2(BusinessDate, true);
            }
            catch (Exception ex)
            {
                Console.WriteLine((ex.InnerException ?? ex).Message);
            }
        }

        private void OnAutoRunTimer_Tick3(object sender, EventArgs e)
        {
            try
            {
                BusinessDate = _jobsRepository.GetBusinessDate("08:00");
                OnExecuteCommand3(BusinessDate, true);
            }
            catch (Exception ex)
            {
                Console.WriteLine((ex.InnerException ?? ex).Message);
            }
        }

        private void OnAutoRunTimer_Tick4(object sender, EventArgs e)
        {
            try
            {
                BusinessDate = _jobsRepository.GetBusinessDate("08:00");
                OnExecuteCommand4(BusinessDate, true);
            }
            catch (Exception ex)
            {
                Console.WriteLine((ex.InnerException ?? ex).Message);
            }
        }


        private bool _isAutoMode=false;

        public bool IsAutoMode
        {
            get { return _isAutoMode; }
            set { 
                _isAutoMode = value;
                RaisePropertyChanged("IsAutoMode");
            }
        }
        
        private void AddAppLog(string status, string description, string message, bool append = true)
        {
            try
            {
                var log = new AppLog()
                {
                    Log_Id = 0,
                    Log_Time = DateTime.Now,
                    Job_Desc = description,
                    Status = status,
                    Message = message,
                };

                if (append)
                {
                    _appLogs.Add(log);
                }
                else
                {
                    _appLogs.Insert(0, log);
                }
                _appLogRepository.InsertAppLog(log);
                ScrollLastLogIntoView();
                if (_appLogs.Count > 100)
                {
                    _appLogs.RemoveAt(0);
                }
                App.DoEvent();
            }
            catch (Exception ex)
            {
                Console.WriteLine((ex.InnerException??ex).Message);
            }
            
        }

        private void ScrollLastLogIntoView()
        {
            if (_appLogs.Count == 0)
            {
                return;
            }

            var dg = _view.FindName("lvLogs") as DataGrid;
            var log = _appLogs[_appLogs.Count - 1];
            if (dg != null)
            {
                dg.ScrollIntoView(log);
            }
        }
    }
}