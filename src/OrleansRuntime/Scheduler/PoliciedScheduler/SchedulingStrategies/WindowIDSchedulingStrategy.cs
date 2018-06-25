using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Orleans.Runtime.Scheduler.SchedulerUtility;

namespace Orleans.Runtime.Scheduler.PoliciedScheduler.SchedulingStrategies
{
    internal class WindowIDSchedulingStrategy : ISchedulingStrategy
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
            // Populate Topology info

            var schedulingContext = context as SchedulingContext;
            var wig = Scheduler.GetWorkItemGroup(schedulingContext);
            if (wig == null)
            {
                var error = string.Format(
                    "WorkItem {0} on context {1} does not match a work item group", workItem, context);
                _logger.Error(ErrorCode.SchedulerQueueWorkItemWrongCall, error);
                throw new InvalidOperationException(error);
            }
        }

        public void OnReceivingDownstreamInstructions(IWorkItem workItem, ISchedulingContext context)
        {
            
        }

        public WorkItemGroup CreateWorkItemGroup(IOrleansTaskScheduler ots, ISchedulingContext context)
        {
            var wig = new WorkItemGroup(ots, context);
            wig.WorkItemManager = new WindowIDWorkItemManager();
            return wig;
        }

        public DownstreamContext CheckForSchedulerHint(ActivationAddress sendingActivationAddress, GrainId upstream)
        {
            return null;
        }

    }

    internal class WindowIDWorkItemManager : IWorkItemManager
     {
        private Queue<Task> workItems { get; set; }
        public ISchedulingStrategy Strategy { get; set; }
        public WindowIDWorkItemManager()
        {
            workItems = new Queue<Task>();
        }

         public void AddToWorkItemQueue(Task task,  WorkItemGroup wig)
        {
            workItems.Enqueue(task);
        }

        public void OnAddWIGToRunQueue(Task task, WorkItemGroup wig)
        {
            var contextObj = task.AsyncState as PriorityContext;
            var priority = contextObj?.Timestamp ?? SchedulerConstants.DEFAULT_PRIORITY;
            if (wig.PriorityContext.Priority < priority)
            {
                wig.PriorityContext.Priority = priority;
                wig.PriorityContext.Ticks = Environment.TickCount;
            }
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

         public void UpdateWIGStatistics() { }

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
