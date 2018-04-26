using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Runtime.Scheduler.PoliciedScheduler.SchedulingStrategies
{
    interface ISchedulingStrategy
    {
        IComparable GetPriority();
        int GetQuantumNumTasks();
        int GetQuantumMillis();
    }
}
