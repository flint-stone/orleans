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

    [Serializable]
    public struct TimestampContext
    {
        public long RequestId;
        public long ConvertedPhysicalTime;
        public long ConvertedLogicalTime;
        public long SLA;
        public long Priority;

        public override string ToString()
        {
            return $"ID: {RequestId} CPT {ConvertedPhysicalTime}, CLT {ConvertedLogicalTime}, SLA {SLA}, Priority {Priority}";
        }
    }
}
