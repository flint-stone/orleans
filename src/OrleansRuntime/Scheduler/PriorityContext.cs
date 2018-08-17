using System;

namespace Orleans.Runtime.Scheduler
{
    internal class PriorityContext
    {
        public long Priority { get; set; }
        public long WindowID { get; set; }
        public ISchedulingContext Context { get; set; }
        public ActivationAddress SourceActivation { get; set; }
        public override String ToString()
        {
            return $"{Context}, Priority : {Priority}, WindowID: {WindowID}, Source: {SourceActivation?.ToString() ?? "null"}" ;
        }
    }

    [Serializable]
    public struct TimestampContext
    {
        public long ConvertedPhysicalTime;
        public long ConvertedLogicalTime;
        public long TimeInQueue;

        public override string ToString()
        {
            return $"CPT {ConvertedPhysicalTime}, CLT {ConvertedLogicalTime}, TimeInQueue {TimeInQueue}";
        }
    }
}
