using System;

namespace Orleans.Runtime.Scheduler
{
    internal class PriorityContext
    {
        public double Priority;
        public ISchedulingContext Context { get; set; }

        public override String ToString()
        {
            return Context + " : " + Priority;
        }
    }
}
