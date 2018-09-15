using System;

namespace Orleans.Runtime.Scheduler
{
    internal class PriorityContext
    {
        public long RequestId { get; set; }
        public long Priority { get; set; }
        public long WindowID { get; set; }
        public ISchedulingContext Context { get; set; }
        public ActivationAddress SourceActivation { get; set; }
        public override String ToString()
        {
            return $"{Context}, RequestId : {RequestId} Priority : {Priority}, WindowID: {WindowID}, Source: {SourceActivation?.ToString() ?? "null"}" ;
        }
    }

    [Serializable]
    public struct TimestampContext
    {
        public long RequestId;
        public long ConvertedPhysicalTime;
        public long ConvertedLogicalTime;
        public long TimeInQueue;

        public override string ToString()
        {
            return $"ID: {RequestId} CPT {ConvertedPhysicalTime}, CLT {ConvertedLogicalTime}, TimeInQueue {TimeInQueue}";
        }
    }
}
