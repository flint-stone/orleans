﻿using System;
using System.Collections.Generic;
using Orleans.Runtime.Scheduler.SchedulerUtility;

namespace Orleans.Runtime.Scheduler
{
    [Serializable]
    public class DownstreamContext
    {
        public Dictionary<string, long> StatsUpdate { get; }

        public long MaximumDownstreamCost { get; }

        public DownstreamContext(Dictionary<string, long> statsCollection, long maximumDownstreamCost = SchedulerConstants.DEFAULT_WIG_EXECUTION_COST)
        {
            StatsUpdate = statsCollection;
            MaximumDownstreamCost = maximumDownstreamCost;
        }
    }
}