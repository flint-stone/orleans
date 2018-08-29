using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans;
using TestGrainInterfaces;

namespace TestGrains
{
    public class TestGrain : Grain, TestGrainInterfaces.ITestGrain
    {
        private static int FibRec(int iter)
        {
            if (iter == 0) return 0;
            if (iter == 1) return 1;
            return FibRec(iter - 1) + FibRec(iter - 2);
        }


        public Task<int> Fib(int num)
        {
            int iters = 1;
            int ret = 0;
            while (iters-- > 0)
            {
                ret = FibRec(num);
            }
            return Task.FromResult(ret);
        }
    }
}
