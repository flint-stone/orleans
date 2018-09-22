using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LinearEstimation
{
    class Program
    {
        public static void Main(string[] args)
        {
            //            //
            //            // In this example we demonstrate linear fitting by f(x|a) = a*exp(0.5*x).
            //            //
            //            // We have:
            //            // * y - vector of experimental data
            //            // * fmatrix -  matrix of basis functions calculated at sample points
            //            //              Actually, we have only one basis function F0 = exp(0.5*x).
            //            //
            //            double[,] fmatrix = new double[,] { { 0.606531 }, { 0.670320 }, { 0.740818 }, { 0.818731 }, { 0.904837 }, { 1.000000 }, { 1.105171 }, { 1.221403 }, { 1.349859 }, { 1.491825 }, { 1.648721 } };
            //            double[] y = new double[] { 1.133719, 1.306522, 1.504604, 1.554663, 1.884638, 2.072436, 2.257285, 2.534068, 2.622017, 2.897713, 3.219371 };
            //            int info;
            //            double[] c;
            //            alglib.lsfitreport rep;
            //
            //            //
            //            // Linear fitting without weights
            //            //
            //            alglib.lsfitlinear(y, fmatrix, out info, out c, out rep);
            //            System.Console.WriteLine("{0}", info); // EXPECTED: 1
            //            System.Console.WriteLine("{0}", alglib.ap.format(c, 4)); // EXPECTED: [1.98650]
            //
            //            //
            //            // Linear fitting with individual weights.
            //            // Slightly different result is returned.
            //            //
            //            double[] w = new double[] { 1.414213, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 };
            //            alglib.lsfitlinearw(y, w, fmatrix, out info, out c, out rep);
            //            System.Console.WriteLine("{0}", info); // EXPECTED: 1
            //            System.Console.WriteLine("{0}", alglib.ap.format(c, 4)); // EXPECTED: [1.983354]
            //            System.Console.ReadLine();
            //            // return 0;
            //
            // In this example we demonstrate linear fitting by f(x|a,b) = a*x+b
            // with simple constraint f(0)=0.
            //
            // We have:
            // * y - vector of experimental data
            // * fmatrix -  matrix of basis functions sampled at [0,1] with step 0.2:
            //                  [ 1.0   0.0 ]
            //                  [ 1.0   0.2 ]
            //                  [ 1.0   0.4 ]
            //                  [ 1.0   0.6 ]
            //                  [ 1.0   0.8 ]
            //                  [ 1.0   1.0 ]
            //              first column contains value of first basis function (constant term)
            //              second column contains second basis function (linear term)
            // * cmatrix -  matrix of linear constraints:
            //                  [ 1.0  0.0  0.0 ]
            //              first two columns contain coefficients before basis functions,
            //              last column contains desired value of their sum.
            //              So [1,0,0] means "1*constant_term + 0*linear_term = 0" 
            //
//            double[] y = new double[] { 0.072436, 0.246944, 0.491263, 0.522300, 0.714064, 0.921929 };
//            double[,] fmatrix = new double[,] { { 1, 0.0 }, { 1, 0.2 }, { 1, 0.4 }, { 1, 0.6 }, { 1, 0.8 }, { 1, 1.0 } };
//            double[,] cmatrix = new double[,] { { 1, 0, 0 } };
            double[] y = new double[] { 2.172436, 2.346944, 2.591263, 2.622300, 2.814064, 3.021929,  0, 0};
            double[,] fmatrix = new double[,] { { 1, 0.0 }, { 1, 0.2 }, { 1, 0.4 }, { 1, 0.6 }, { 1, 0.8 }, { 1, 1.0 }, { 0, 0.0 }, { 0, 0.0 } };
            //double[,] cmatrix = new double[,] { { 1, 0, 0 } };
            int info;
            double[] c;
            alglib.lsfitreport rep;

            //, 0
            // Constrained fitting without weights
            //
            alglib.lsfitlinear(y, fmatrix, out info, out c, out rep);
            //alglib.lsfitlinearc(y, fmatrix, cmatrix, out info, out c, out rep);
            System.Console.WriteLine("{0}", info); // EXPECTED: 1
            System.Console.WriteLine("{0}", alglib.ap.format(c, 3)); // EXPECTED: [0,0.932933]
            System.Console.WriteLine("{0}", string.Join(",",rep.errpar));

            //
            // Constrained fitting with individual weights
            //
            double[] w = new double[] { 1, 1.414213, 1, 1, 1, 1 };
            alglib.lsfitlinearw(y, w, fmatrix, out info, out c, out rep);
            //alglib.lsfitlinearwc(y, w, fmatrix, cmatrix, out info, out c, out rep);
            System.Console.WriteLine("{0}", info); // EXPECTED: 1
            System.Console.WriteLine("{0}", alglib.ap.format(c, 3)); // EXPECTED: [0,0.938322]
            System.Console.ReadLine();
            
        }
    }
}
