using System;

namespace Orleans.Runtime.Scheduler
{
    internal class PriorityContext
    {
        public long Priority { get; set; }
        public ISchedulingContext Context { get; set; }
        public ActivationAddress SourceActivation { get; set; }
        public override String ToString()
        {
            return Context + " : " + Priority + "Source: " + (SourceActivation?.ToString() ?? "null");
        }
    }
}
