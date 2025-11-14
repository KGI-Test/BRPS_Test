using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Navigation;
using System.Windows.Shapes;

namespace BRPS.Views.Controls
{
    /// <summary>
    /// Interaction logic for ScheduledJobEditor.xaml
    /// </summary>
    public partial class ScheduledJobView : UserControl
    {
        private bool _viewMode;

        public ScheduledJobView()
        {
            InitializeComponent();
        }
        

        public bool ViewMode
        {
            get { return _viewMode; }
            set
            {
                _viewMode = value;
                txtDescription.IsReadOnly = value;
                cboOccurence.IsReadOnly = value;
                cboOccurence.IsEditable = value;
                txtInverval.IsReadOnly = value;
                txtFromTime.IsReadOnly = value;
                txtEndTime.IsReadOnly = value;
                txtProcess.IsReadOnly = value;               

                paramsExpander.IsExpanded = !value;
                lvParameters.IsReadOnly = value;
                lvParameters.CanUserAddRows = !value;
                lvParameters.CanUserDeleteRows = !value;
                lvParameters.UpdateLayout();
            }
        }

       
    }
}
