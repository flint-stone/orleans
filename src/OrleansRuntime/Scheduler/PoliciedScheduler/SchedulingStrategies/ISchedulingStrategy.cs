using System;

namespace Orleans.Runtime.Scheduler.PoliciedScheduler.SchedulingStrategies
{
    internal interface ISchedulingStrategy
    {
        IOrleansTaskScheduler Scheduler { get; set; }

        IComparable GetPriority(IWorkItem workItem);

        void Initialization();

        void OnWorkItemInsert(IWorkItem workItem, WorkItemGroup wig);

        void OnReceivingControllerInstructions(IWorkItem workItem, ISchedulingContext context);

        WorkItemGroup CreateWorkItemGroup(IOrleansTaskScheduler ots, ISchedulingContext context);

        long FetchWorkItemMetric(WorkItemGroup workItem);

        void PutWorkItemMetric(WorkItemGroup workItemGroup, Object metric);

        long PeekNextDeadline();

    }
}
