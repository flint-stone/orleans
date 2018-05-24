using System;
using System.Threading.Tasks;

namespace Orleans.Runtime.Scheduler.PoliciedScheduler.SchedulingStrategies
{
    interface IWorkItemManager
    {
        ISchedulingStrategy Strategy { get; set; }

        void AddToWorkItemQueue(Task task, WorkItemGroup wig);

        void OnAddWIGToRunQueue(Task task, WorkItemGroup wig);

        void OnClosingWIG();

        Task GetNextTaskForExecution();

        void UpdateWIGStatistics();

        int CountWIGTasks();

        Task GetOldestTask();

        String GetWorkItemQueueStatus();

        void OnReAddWIGToRunQueue(WorkItemGroup wig);
    }
}
