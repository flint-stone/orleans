﻿

namespace Orleans.Runtime.Scheduler.SchedulerUtility
{
    internal static class SchedulerConstants
    {
        internal const long DEFAULT_PRIORITY = default(long);
        internal const long DEFAULT_WINDOW_ID = default(long);
        internal const long DEFAULT_REQUEST_ID = default(long);
        internal const long DEFAULT_WIG_EXECUTION_COST = default(long);
        internal const long DEFAULT_DATAFLOW_SLA = 5000000;
        internal const int MEASUREMENT_PERIOD_WORKITEM_COUNT = 10;
        internal const int STATS_COUNTER_QUEUE_SIZE = 100;
        internal const int DEFAULT_TASK_TRACKING_ID = -1;
        internal const int PRIORITY_GRANULARITY_TICKS = 1;

        internal static int SCHEDULING_QUANTUM_MINIMUM_MILLIS = 50;
        // internal const long TASK_LENGTH_SAMPLING_INTERVAL
    }
}