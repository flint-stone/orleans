using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Orleans.Runtime.Scheduler.SchedulerUtility;

namespace Orleans.Runtime.Scheduler.PoliciedScheduler.SchedulingStrategies
{
    internal class WindowIDSchedulingStrategy : ISchedulingStrategy
    {
        private readonly LoggerImpl logger = LogManager.GetLogger("Scheduler.PoliciedScheduler.SchedulingStrategies", LoggerType.Runtime);

        #region Tenancies

        private Dictionary<short, Tuple<ulong, HashSet<ulong>>> tenants;
        private Dictionary<short, long> timeLimitsOnTenants;
        private Dictionary<WorkItemGroup, FixedSizedQueue<double>> tenantStatCounters;
        private const int MaximumStatCounterSize = 100;
        // TODO: FIX LATER
        private int statCollectionCounter = 100;

        #endregion

        public IOrleansTaskScheduler Scheduler { get; set; }

        public IComparable GetPriority(IWorkItem workItem)
        {
            if (Scheduler.GetWorkItemGroup(workItem.SchedulingContext) != null) return workItem.PriorityContext;
            return SchedulerConstants.DEFAULT_PRIORITY;
        }

        public void Initialization()
        {
            tenants = new Dictionary<short, Tuple<ulong, HashSet<ulong>>>();
            timeLimitsOnTenants = new Dictionary<short, long>();
            tenantStatCounters = new Dictionary<WorkItemGroup, FixedSizedQueue<double>>();
        }

        public void OnWorkItemInsert(IWorkItem workItem, WorkItemGroup wig) { }

        public void OnReceivingControllerInstructions(IWorkItem workItem, ISchedulingContext context)
        {
            // Populate Topology info
            var controllerContext = ((InvokeWorkItem)workItem).ControllerContext;

            ulong controllerId = ((InvokeWorkItem)workItem).SourceActivation.Grain.Key.N1;
            var schedulingContext = context as SchedulingContext;
            if (tenants.ContainsKey(controllerContext.AppId))
            {
                tenants[controllerContext.AppId].Item2.Add(schedulingContext.Activation.Grain.Key.N1);
            }
            else
            {
                // Initialize entries in *ALL* per-dataflow maps
                tenants.Add(controllerContext.AppId, new Tuple<ulong, HashSet<ulong>>(controllerId, new HashSet<ulong>()));
                timeLimitsOnTenants.Add(controllerContext.AppId, controllerContext.Time);

                tenants[controllerContext.AppId].Item2.Add(schedulingContext.Activation.Grain.Key.N1);
            }
            var wig = Scheduler.GetWorkItemGroup(schedulingContext);
            if (wig == null)
            {
                var error = string.Format(
                    "WorkItem {0} on context {1} does not match a work item group", workItem, context);
                logger.Error(ErrorCode.SchedulerQueueWorkItemWrongCall, error);
                throw new InvalidOperationException(error);
            }
            if (!tenantStatCounters.ContainsKey(wig))
            {
                tenantStatCounters.Add(wig, new FixedSizedQueue<double>(MaximumStatCounterSize));
            }
        }

        public WorkItemGroup CreateWorkItemGroup(IOrleansTaskScheduler ots, ISchedulingContext context)
        {
            var wig = new WorkItemGroup(ots, context);
            wig.WorkItemManager = new WindowIDWorkItemManager();
            return wig;
        }

        public object FetchWorkItemMetric(WorkItemGroup workItem)
        {
            throw new NotImplementedException();
        }

        public void PutWorkItemMetric(WorkItemGroup workItemGroup, object metric)
        {
            throw new NotImplementedException();
        }

        public long PeekNextDeadline()
        {
            throw new NotImplementedException();
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
