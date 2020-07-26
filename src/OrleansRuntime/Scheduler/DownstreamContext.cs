using System;
using System.Collections.Generic;
using System.Linq;
using Orleans.Runtime.Scheduler.SchedulerUtility;

namespace Orleans.Runtime.Scheduler
{
    [Serializable]
    public class DownstreamContext
    {
        public long MaximumDownstreamCost { get; set; }
        public long LocalExecutionCost { get; set; }
        public long ExpectedSchedulerQueuingDelay { get; set; } // Disabled for now
        public long InboundMessageQueueingDelay { get; set;  }
        public long RemoteDeliveryDelay { get; set; } // Only if it is remote

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
            return String.Format(
                "Maximum possible downstream cost {0}. Details: [Downstream {1} LocalExecution {2} SchedulerQueue {3} InboundQueue {4} RemoteDelivery {5}]"
                , DownStreamCostSum, MaximumDownstreamCost, LocalExecutionCost, ExpectedSchedulerQueuingDelay, InboundMessageQueueingDelay,
                RemoteDeliveryDelay);
        }
    }
}
