using System;
using System.Threading.Tasks;

namespace Orleans.Runtime.Scheduler.PoliciedScheduler.SchedulingStrategies
{
    interface IWorkItemManager
    {
        void AddToWorkItemQueue(Task task, WorkItemGroup wig);

        void OnAddWIGToRunQueue(Task task, WorkItemGroup wig);

        void OnClosingWIG();

        Task GetNextTaskForExecution();

        int CountWIGTasks();

        Task GetOldestTask();

        String GetWorkItemQueueStatus();

        void OnReAddWIGToRunQueue();
    }
}
