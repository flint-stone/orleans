using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans;

namespace TestGrainInterfaces
{
    public interface ITestGrain : IGrainWithIntegerKey
    {
        Task<int> Fib(int num);
    }
}
