using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Orleans.Runtime.Scheduler.PoliciedScheduler.SchedulingStrategies
{
    internal interface ISchedulingStrategy
    {
        IOrleansTaskScheduler Scheduler { get; set; }

        #region IOrleansTaskScheduler

        void CollectStatistics();
        IComparable GetPriority(IWorkItem workItem);
        int GetQuantumNumTasks();
        int GetQuantumMillis();
        void Initialization();
        void OnWorkItemInsert(IWorkItem workItem, WorkItemGroup wig);
        void OnReceivingControllerInstructions(IWorkItem workItem, ISchedulingContext context);
        #endregion

        #region WorkItemGroup

        IEnumerable CreateWorkItemQueue();
        void AddToWorkItemQueue(Task task, IEnumerable workItems, WorkItemGroup wig);
        void OnAddWIGToRunQueue(Task task, WorkItemGroup wig);
        void OnClosingWIG(IEnumerable workItems);
        Task GetNextTaskForExecution(IEnumerable workItems);
        int CountWIGTasks(IEnumerable workItems);
        Task GetOldestTask(IEnumerable workItems);
        String GetWorkItemQueueStatus(IEnumerable workItems);
        #endregion

    }
}
