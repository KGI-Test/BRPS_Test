using BRPS.Managers;
using BRPS.Model;
using BRPS.Views.Controls;
using BRPS.Repositories;
using BRPS.ViewModels;
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
using System.Collections.ObjectModel;

namespace BRPS.Views
{
    /// <summary>
    /// Interaction logic for MainWindow.xaml
    /// </summary>
    public partial class MainWindow : Window
    {
        System.Windows.Threading.DispatcherTimer dispatcherTimer = new System.Windows.Threading.DispatcherTimer(); 

        public MainWindow()
        {
            InitializeComponent();
        }

        private void btnClose_Click(object sender, RoutedEventArgs e)
        {
            this.Close();
        }

        private void btnTest_Click(object sender, RoutedEventArgs e)
        {
            var mv = lvLogs.DataContext as MainViewModel;
            var cv=mv.AppLogCV.SourceCollection as ObservableCollection<AppLog>;
            lvLogs.ScrollIntoView(cv[cv.Count-1]);
        }
    }
}
