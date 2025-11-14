using BRPS.Model.Extensions;
using BRPS.ViewModels;
using System;
using System.Windows;

namespace BRPS.Views
{
    /// <summary>
    /// Description for EditSheduledJobVM.
    /// </summary>
    public partial class EditScheduledJobWindow : Window
    {
        /// <summary>
        /// Initializes a new instance of the EditSheduledJobVM class.
        /// </summary>
        public EditScheduledJobWindow() 
        {
            InitializeComponent();           
        }

        private void btnClose_Click(object sender, RoutedEventArgs e)
        {
            this.Close();
        }

        //private void btnTest_Click(object sender, RoutedEventArgs e)
        //{
        //    var dc = this.DataContext;

        //}
       
    }
}