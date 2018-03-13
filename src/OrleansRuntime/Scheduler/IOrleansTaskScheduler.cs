using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans.Runtime.Configuration;

namespace Orleans.Runtime.Scheduler
{
    internal interface IOrleansTaskScheduler : ITaskScheduler, IHealthCheckParticipant
    {
        WorkQueue RunQueue { get; }
        WorkerPool Pool { get; }
        LimitValue MaxPendingItemsLimit { get; }
        TimeSpan DelayWarningThreshold { get; }
        TaskScheduler Instance { get; }
        TimeSpan TurnWarningLength { get; }
        int RunQueueLength { get; }
        int WorkItemGroupCount { get; }
        float AverageRunQueueLengthLevelTwo { get; }
        float AverageEnqueuedLevelTwo { get; }
        float AverageArrivalRateLevelTwo { get; }
        float SumRunQueueLengthLevelTwo { get; }
        float SumEnqueuedLevelTwo { get; }
        float SumArrivalRateLevelTwo { get; }
        int MaximumConcurrencyLevel { get; }
        void Start();
        void StopApplicationTurns();
        void Stop();
        void QueueWorkItem(IWorkItem workItem, ISchedulingContext context);
        WorkItemGroup RegisterWorkContext(ISchedulingContext context);
        void UnregisterWorkContext(ISchedulingContext context);
        WorkItemGroup GetWorkItemGroup(ISchedulingContext context);
        void CheckSchedulingContextValidity(ISchedulingContext context);
        TaskScheduler GetTaskScheduler(ISchedulingContext context);

        /// <summary>
        /// Run the specified task synchronously on the current thread
        /// </summary>
        /// <param name="task"><c>Task</c> to be executed</param>
        void RunTask(Task task);

        bool CheckHealth(DateTime lastCheckTime);
        void PrintStatistics();
        void DumpSchedulerStatus(bool alwaysOutput = true);
    }
}
