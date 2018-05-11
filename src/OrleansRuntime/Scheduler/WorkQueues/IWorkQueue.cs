using System;
using System.Text;
using System.Threading;

namespace Orleans.Runtime.Scheduler
{
    internal interface IWorkQueue
    {
        int Length { get; }
        void Add(IWorkItem workItem);
        IWorkItem Get(CancellationToken ct, TimeSpan timeout);
        IWorkItem GetSystem(CancellationToken ct, TimeSpan timeout);
        void DumpStatus(StringBuilder sb);
        void RunDown();
        IWorkItem Peek();
    }
}
