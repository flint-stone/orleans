using System;
using System.Threading.Tasks;

namespace Orleans.Runtime.Scheduler.PoliciedScheduler.SchedulingStrategies
{
    internal interface ISchedulingStrategy
    {
        IOrleansTaskScheduler Scheduler { get; set; }

        void Initialization();

        void OnWorkItemInsert(IWorkItem workItem, WorkItemGroup wig);

        void OnReceivingControllerInstructions(IWorkItem workItem, ISchedulingContext context);

        WorkItemGroup CreateWorkItemGroup(IOrleansTaskScheduler ots, ISchedulingContext context);
    }

    interface IWorkItemManager
    {
        void AddToWorkItemQueue(Task task, WorkItemGroup wig);

        void OnAddWIGToRunQueue(Task task, WorkItemGroup wig);

        void OnClosingWIG();

        Task GetNextTaskForExecution();

        void OnFinishingCurrentTurn();

        void AddNewStat(Task task, PriorityContext contextObj, TimeSpan taskLength);

        int CountWIGTasks();

        Task GetOldestTask();

        String GetWorkItemQueueStatus();

        void OnReAddWIGToRunQueue(WorkItemGroup wig);
    }

}
