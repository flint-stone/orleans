using System;
using System.Collections.Generic;
using System.Linq;
using Orleans.Runtime.Scheduler.SchedulerUtility;

namespace Orleans.Runtime.Scheduler
{
    [Serializable]
    public class DownstreamContext
    {
        // public Dictionary<string, long> ExecutionCostByTaskType { get; }

        // public long AverageQueueingDelayInTicks { get; }
        public long MaximumDownstreamCost { get; }
        public long LocalExecutionCost { get; }
        public long ExpectedSchedulerQueuingDelay { get; } // Disabled for now
        public long InboundMessageQueueingDelay { get; }
        public long RemoteDeliveryDelay { get; } // Only if it is remote

        public long DownStreamCostSum => MaximumDownstreamCost +
                                         LocalExecutionCost +
                                         ExpectedSchedulerQueuingDelay +
                                         InboundMessageQueueingDelay +
                                         RemoteDeliveryDelay;

        public DownstreamContext(long maximumDownstreamCost, 
            long localExecutionCost,
            long expectedSchedulerQueuingDelay,
            long inboundMessageQueueingDelay,
            long remoteDeliveryDelay)
        {
            MaximumDownstreamCost = maximumDownstreamCost;
            LocalExecutionCost = localExecutionCost;
            ExpectedSchedulerQueuingDelay = expectedSchedulerQueuingDelay;
            InboundMessageQueueingDelay = inboundMessageQueueingDelay;
            RemoteDeliveryDelay = remoteDeliveryDelay;
        }

        public override string ToString()
        {
//            return String.Format("[{0}, {1}]", string.Join(",", ExecutionCostByTaskType.Select(kv => kv.Key + "->" + kv.Value)),
//                MaximumDownstreamCost);
            return String.Format(
                "Maximum possible downstream cost {0}. Details: [Downstream {1} LocalExecution {2} SchedulerQueue {3} InboundQueue {4} RemoteDelivery {5}]"
                , DownStreamCostSum, MaximumDownstreamCost, LocalExecutionCost, ExpectedSchedulerQueuingDelay, InboundMessageQueueingDelay,
                RemoteDeliveryDelay);
        }
    }
}
