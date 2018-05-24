using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Orleans.Runtime.Scheduler.SchedulerUtility;

namespace Orleans.Runtime.Scheduler.PoliciedScheduler.SchedulingStrategies
{
    internal class TestSchedulingStrategy : ISchedulingStrategy
    {
        private readonly LoggerImpl logger = LogManager.GetLogger("Scheduler.PoliciedScheduler.SchedulingStrategies", LoggerType.Runtime);

        #region Tenancies

        private Dictionary<short, Tuple<ulong, HashSet<ulong>>> tenants;
        private Dictionary<short, long> timeLimitsOnTenants;
        private Dictionary<WorkItemGroup, FixedSizedQueue<double>> tenantStatCounters;
        private const int MaximumStatCounterSize = 100;
        
        #endregion

        public IOrleansTaskScheduler Scheduler { get; set; }

        public IComparable GetPriority(IWorkItem workItem)
        {
            if (Scheduler.GetWorkItemGroup(workItem.SchedulingContext)!=null ) return workItem.PriorityContext;
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
            if (!tenantStatCounters.ContainsKey(wig)) tenantStatCounters.Add(wig, new FixedSizedQueue<double>(MaximumStatCounterSize));
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

        public void PutWorkItemMetric(WorkItemGroup workItemGroup, object metric)
        {
            throw new NotImplementedException();
        }

        public long PeekNextDeadline()
        {
            throw new NotImplementedException();
        }

    }

    internal class TestWorkItemManager : IWorkItemManager
    {
        private SortedDictionary<double, Queue<Task>> workItems;
        public ISchedulingStrategy Strategy { get; set; }
        public TestWorkItemManager()
        {
            workItems = new SortedDictionary<double, Queue<Task>>();
        }

        public void AddToWorkItemQueue(Task task, WorkItemGroup wig)
        {
            var priority = workItems.Count > 0 ? workItems.Keys.First() : 0.0;
            var contextObj = task.AsyncState as PriorityContext;
            if (contextObj != null)
            {
                // TODO: FIX LATER
                priority = contextObj.Timestamp == SchedulerConstants.DEFAULT_PRIORITY ? wig.PriorityContext : contextObj.Timestamp;
            }
            if (!workItems.ContainsKey(priority))
            {
                workItems.Add(priority, new Queue<Task>());
            }
            workItems[priority].Enqueue(task);
        }

        public void OnAddWIGToRunQueue(Task task, WorkItemGroup wig)
        {
            var contextObj = task.AsyncState as PriorityContext;
            var priority = contextObj?.Timestamp ?? SchedulerConstants.DEFAULT_PRIORITY;
            if (wig.PriorityContext < priority)
            {
                wig.PriorityContext = priority;
            }
        }

        public void OnClosingWIG()
        {
            foreach (var kv in workItems)
            {
                foreach (Task task in kv.Value)
                {
                    // Ignore all queued Tasks, so in case they are faulted they will not cause UnobservedException.
                    task.Ignore();
                }
                workItems[kv.Key].Clear();
                workItems.Remove(kv.Key);
            }
        }

        public Task GetNextTaskForExecution()
        {
            var queue = workItems.First().Value;
            if (queue.Count > 0)
            {
                return queue.Dequeue();
            }

            // finish current priority, break and take wig off the queue
            workItems.Remove(workItems.Keys.First());
            return null;
        }

        public void UpdateWIGStatistics() { }

        public int CountWIGTasks()
        {
            return workItems.Values.Select(x => x.Count).Sum();
        }

        public Task GetOldestTask()
        {
            return workItems.Values.Select(x => x.Count).Sum() >= 0
                ? workItems[workItems.Keys.First()].Peek()
                : null;
        }

        public string GetWorkItemQueueStatus()
        {
            return string.Join("|||",
                workItems.Select(x =>
                    x.Key + ":" + string.Join(",",
                        x.Value.Select(y =>
                            {
                                var contextObj = y.AsyncState as PriorityContext;
                                return "<" + y.ToString() + "-" +
                                       (contextObj?.Timestamp.ToString() ?? "null") + ">";
                            }
                        ))));
        }

        public void OnReAddWIGToRunQueue(WorkItemGroup wig) { }

    }
}
