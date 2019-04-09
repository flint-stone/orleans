using System;

namespace Orleans.Runtime.Scheduler
{
    internal class PriorityContext
    {
        public long RequestId { get; set; }
        public long GlobalPriority { get; set; }
        public long LocalPriority { get; set; }
        public ISchedulingContext Context { get; set; }
        public ActivationAddress SourceActivation { get; set; }
        public override String ToString()
        {
            return $"{Context}, RequestId : {RequestId} GlobalPriority : {GlobalPriority}, LocalPriority: {LocalPriority}, Source: {SourceActivation?.ToString() ?? "null"}" ;
        }
    }

    public interface RuntimePriorityContext
    {
        long Id { get; }
        long GlobalPriority { get; }
        long LocalPriority { get; }
    }

}
