

namespace Orleans.Runtime.Scheduler.SchedulerUtility
{
    internal static class SchedulerConstants
    {
        internal const long DEFAULT_PRIORITY = 0L;
        internal const long DEFAULT_WIG_EXECUTION_COST = 0L;
        internal const int DEFAULT_TASK_QUANTUM_MILLIS = 100;
        internal const int DEFAULT_TASK_QUANTUM_NUM_TASKS = 0;
        internal const long DEFAULT_DATAFLOW_SLA = 5000000;
        internal const long DEFAULT_WINODW_SIZE = 100000000;
        internal const int MEASUREMENT_PERIOD_WORKITEM_COUNT = 100;
        internal const int STATS_COUNTER_QUEUE_SIZE = 30;
    }
}
