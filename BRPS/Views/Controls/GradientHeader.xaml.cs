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
    /// Interaction logic for GradientHeader.xaml
    /// </summary>
    public partial class GradientHeader : UserControl
    {
        public GradientHeader()
        {
            InitializeComponent();
        }

        public Color StartColor
        {
            get
            {
                return GradientStartColor.Color;
            }
            set
            {
                GradientStartColor.Color = value;
            }
        }

        public Color EndColor
        {
            get
            {
                return GradientEndColor.Color;
            }
            set
            {
                GradientEndColor.Color = value;
            }
        }
    }
}
