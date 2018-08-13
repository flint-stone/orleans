using System;
using System.Threading.Tasks;

namespace Orleans.Runtime.Scheduler.PoliciedScheduler.SchedulingStrategies
{
    interface IWorkItemManager
    {
        void AddToWorkItemQueue(Task task, WorkItemGroup wig);

        bool OnAddWIGToRunQueue(Task task, WorkItemGroup wig);

        void OnClosingWIG();

        Task GetNextTaskForExecution();

        void OnFinishingWIGTurn();

        int CountWIGTasks();

        Task GetOldestTask();

        String GetWorkItemQueueStatus();

        void OnReAddWIGToRunQueue(WorkItemGroup wig);
    }
}
