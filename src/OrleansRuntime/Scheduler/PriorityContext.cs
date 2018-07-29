using System;

namespace Orleans.Runtime.Scheduler
{
    internal class PriorityContext
    {
        public long Timestamp;

        public long FromApp;

        public ISchedulingContext Context { get; set; }
        public ActivationAddress SourceActivation { get; set; }
        public override String ToString()
        {
            return Context + " : " + Timestamp + "Source: " + (SourceActivation?.ToString() ?? "null");
        }
    }
}
