using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;

namespace BRPS
{
    /// <summary>
    /// Interaction logic for App.xaml
    /// </summary>
    public partial class App : Application
    {
        public static void DoEvent()
        {
            App.Current.Dispatcher.Invoke(new Action( ()=> {
                }),System.Windows.Threading.DispatcherPriority.Background);

        }
    }
}
