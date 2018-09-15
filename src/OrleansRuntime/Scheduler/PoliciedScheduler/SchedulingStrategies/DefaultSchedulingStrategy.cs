using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Orleans.Runtime.Scheduler.PoliciedScheduler.SchedulingStrategies
{
    internal class DefaultSchedulingStrategy : ISchedulingStrategy
    {
        private LoggerImpl _logger;

        public IOrleansTaskScheduler Scheduler { get; set; }

        public void Initialization()
        {
            _logger = LogManager.GetLogger(this.GetType().FullName, LoggerType.Runtime);
        }

        public void OnWorkItemInsert(IWorkItem workItem, WorkItemGroup wig) { }

        public void OnReceivingControllerInstructions(IWorkItem workItem, ISchedulingContext context)
        {
            throw new NotImplementedException();
        }

        public void OnReceivingDownstreamInstructions(IWorkItem workItem, ISchedulingContext context)
        {
            throw new NotImplementedException();
        }

        public WorkItemGroup CreateWorkItemGroup(IOrleansTaskScheduler ots, ISchedulingContext context)
        {
            var wig = new WorkItemGroup(ots, context);
            wig.WorkItemManager = new DefaultWorkItemManager();
            return wig;
        }

        public DownstreamContext CheckForSchedulerHint(ActivationAddress sendingActivationAddress, GrainId upstream)
        {
            return null;
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

         public bool OnAddWIGToRunQueue(Task task, WorkItemGroup wig)
         {
             return false;
         }

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

         public void OnCompleteTask(PriorityContext context, TimeSpan taskLength) { }

         public void OnFinishingWIGTurn() { }

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

     }
}
