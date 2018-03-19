using System;

namespace Orleans.Runtime.Scheduler
{
    internal class PriorityContext
    {
        public double TimeRemain;
        public ISchedulingContext Context { get; set; }

        public override String ToString()
        {
            return Context + " : " + TimeRemain;
        }
    }
}
