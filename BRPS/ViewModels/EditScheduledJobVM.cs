using BRPS.Model;
using BRPS.Model.Extensions;
using BRPS.Repositories;
using GalaSoft.MvvmLight;
using GalaSoft.MvvmLight.Command;
using GalaSoft.MvvmLight.Messaging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;

namespace BRPS.ViewModels
{
    public class EditScheduledJobVM : ViewModelBase
    {
        private IScheduledJobRepository _jobsRepository;
        private ScheduledJobEx scheduledJob;

        public int Job_Id { get; set; }

        public EditScheduledJobVM(IScheduledJobRepository jobsRepository)
        {
            _jobsRepository = jobsRepository;
            Messenger.Default.Register<EditScheduledJobMessage>(this, (msg) => {ScheduledJob = msg.EditScheduledJob; ResetScheduledJobInfo();});            
        }

        private void ResetScheduledJobInfo()
        {
            try
            {
                ScheduledJob = scheduledJob ?? new ScheduledJobEx(new ScheduledJob());
                Job_Id = ScheduledJob.Job.Job_Id;
                if (Job_Id != 0)
                {
                    ScheduledJob.Job = _jobsRepository.GetScheduledJobByJobId(Job_Id);
                }
                RaisePropertyChanged("ScheduledJob");
            }
            catch (Exception ex)
            {
                Console.WriteLine((ex.InnerException ?? ex).Message);
            }                   
        }

        public ScheduledJobEx ScheduledJob
        {
            get { return scheduledJob; }
            set { 
                scheduledJob = value;
                RaisePropertyChanged("ScheduledJob");
            }
        }

        private RelayCommand _saveCommand;

        /// <summary>
        /// Gets the SaveCommand.
        /// </summary>
        public RelayCommand SaveCommand
        {
            get
            {
                return _saveCommand ?? (_saveCommand = new RelayCommand(OnSaveCommand));
            }
        }

        private void OnSaveCommand()
        {
            try
            {
                scheduledJob.UpdateScheduledJobParams();
                if (Job_Id == 0)
                {
                    _jobsRepository.InsertScheduledJob(scheduledJob.Job);
                    MessageBox.Show("The record has been created sucessfully.", "Scheduled Job Creation", MessageBoxButton.OK, MessageBoxImage.Information);
                }
                else
                {
                    _jobsRepository.EditScheduledJob(scheduledJob.Job);
                    MessageBox.Show("The record has been updated sucessfully.", "Scheduled Job Modification", MessageBoxButton.OK, MessageBoxImage.Information);
                }

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                MessageBox.Show("Error in saving record. Casue: " +( ex.InnerException??ex).Message, "Save Scheduled Job", 
                                MessageBoxButton.OK, MessageBoxImage.Error);
            }
        }

        private RelayCommand _cancleCommand;

        /// <summary>
        /// Gets the CancelCommand.
        /// </summary>
        public RelayCommand CancelCommand
        {
            get
            {
                return _cancleCommand  ?? (_cancleCommand = new RelayCommand(OnCancelCommand));
            }
        }

        private void OnCancelCommand()
        {
            try
            {
                ResetScheduledJobInfo();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }            
        }
    }

    public class EditScheduledJobMessage : MessageBase
    {
        public EditScheduledJobMessage(ScheduledJobEx editScheduledJob)
        {
            EditScheduledJob = editScheduledJob;
        }

        public ScheduledJobEx EditScheduledJob { get; set; }
    }
}
