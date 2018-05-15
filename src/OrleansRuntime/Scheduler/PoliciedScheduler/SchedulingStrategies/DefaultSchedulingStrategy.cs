using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Orleans.Runtime.Scheduler.PoliciedScheduler.SchedulingStrategies
{
    internal class DefaultSchedulingStrategy : ISchedulingStrategy
    {
        private readonly LoggerImpl logger = LogManager.GetLogger("Scheduler.PoliciedScheduler.SchedulingStrategies", LoggerType.Runtime);

        public IOrleansTaskScheduler Scheduler { get; set; }

        public IComparable GetPriority(IWorkItem workItem)
        {
            throw new NotImplementedException();
        }

        public void Initialization()
        {
            throw new NotImplementedException();
        }

        public void OnWorkItemInsert(IWorkItem workItem, WorkItemGroup wig) { }

        public void OnReceivingControllerInstructions(IWorkItem workItem, ISchedulingContext context)
        {
            throw new NotImplementedException();
        }

        public WorkItemGroup CreateWorkItemGroup(IOrleansTaskScheduler ots, ISchedulingContext context)
        {
            var wig = new WorkItemGroup(ots, context);
            wig.WorkItemManager = new TestWorkItemManager();
            return wig;
        }

        public long FetchWorkItemMetric(WorkItemGroup workItem)
        {
            throw new NotImplementedException();
        }

        public void SetWorkItemMetric(WorkItemGroup workItem, long metric)
        {
            throw new NotImplementedException();
        }

        public long PeekNextDeadline()
        {
            throw new NotImplementedException();
        }
    }

    internal class DefaultWorkItemManager : IWorkItemManager
     {
        private Queue<Task> workItems { get; set; }
        public ISchedulingStrategy Strategy { get; set; }
        public DefaultWorkItemManager()
        {
            workItems = new Queue<Task>();
        }

         public void AddToWorkItemQueue(Task task,  WorkItemGroup wig)
        {
            workItems.Enqueue(task);
        }

         public void OnAddWIGToRunQueue(Task task, WorkItemGroup wig) { }

        public void OnClosingWIG()
        {
            foreach (var workItem in workItems) workItem.Ignore();
            workItems.Clear();
        }

        public Task GetNextTaskForExecution()
        {
            if (!workItems.Any()) return null;
            return workItems.Dequeue();
        }

        public int CountWIGTasks()
        {
            return workItems.Count();
        }

        public Task GetOldestTask()
        {
            return workItems.Any() ? workItems.Peek() : null;
        }

        public string GetWorkItemQueueStatus()
        {
            return string.Join(",", workItems);
        }

         public void OnReAddWIGToRunQueue(WorkItemGroup wig) { }

         public void OnReAddWIGToRunQueue() { }
     }
}
