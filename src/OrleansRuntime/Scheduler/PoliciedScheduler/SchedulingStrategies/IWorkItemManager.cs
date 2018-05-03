using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Runtime.Scheduler.PoliciedScheduler.SchedulingStrategies
{
    interface IWorkItemManager
    {
        #region WorkItemGroup

        IEnumerable CreateWorkItemQueue();
        void AddToWorkItemQueue(Task task, WorkItemGroup wig);
        void OnAddWIGToRunQueue(Task task, WorkItemGroup wig);
        void OnClosingWIG();
        Task GetNextTaskForExecution();
        int CountWIGTasks();
        Task GetOldestTask();
        String GetWorkItemQueueStatus();

        #endregion
    }
}
