using System;
using System.CodeDom;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Orleans.Runtime.Scheduler.Utility;

namespace Orleans.Runtime.Scheduler.PoliciedScheduler.SchedulingStrategies
{
    internal class DefaultSchedulingStrategy : ISchedulingStrategy
    {
        private const double DEFAULT_PRIORITY = 0.0;
        private const int DEFAULT_TASK_QUANTUM_MILLIS = 100;
        private const int DEFAULT_TASK_QUANTUM_NUM_TASKS = 0;


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

        #region IOrleansTaskScheduler
        public void CollectStatistics()
        {
            return;
        }

        public IComparable GetPriority(IWorkItem workItem)
        {
            if (Scheduler.GetWorkItemGroup(workItem.SchedulingContext) != null) return workItem.PriorityContext;
            return DEFAULT_PRIORITY;
        }

        public int GetQuantumNumTasks()
        {
            return DEFAULT_TASK_QUANTUM_NUM_TASKS;
        }

        public int GetQuantumMillis()
        {
            return DEFAULT_TASK_QUANTUM_MILLIS;
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
        #endregion

        #region WorkItemGroup 
        // Concurrent execution starts beflow

        public IEnumerable CreateWorkItemQueue()
        {
            return new Queue<Task>();
        }

        public void AddToWorkItemQueue(Task task, IEnumerable workItems, WorkItemGroup wig)
        {
            ((Queue<Task>) workItems).Enqueue(task);
        }

        public void OnAddWIGToRunQueue(Task task, WorkItemGroup wig)
        {
            var contextObj = task.AsyncState as PriorityContext;
            var priority = contextObj?.Priority ?? 0.0;
            if (wig.PriorityContext < priority)
            {
                wig.PriorityContext = priority;
            }
        }

        public void OnClosingWIG(IEnumerable workItems)
        {
            foreach(var workItem in (Queue<Task>)workItems) workItem.Ignore();
            ((Queue<Task>)workItems).Clear();
        }

        public Task GetNextTaskForExecution(IEnumerable workItems)
        {
            if (!((Queue<Task>)workItems).Any()) return null;
            return ((Queue<Task>) workItems).Dequeue();
        }

        public int CountWIGTasks(IEnumerable workItems)
        {
            return ((Queue<Task>)workItems).Count();
        }

        public Task GetOldestTask(IEnumerable workItems)
        {
            return ((Queue<Task>)workItems).Any()? ((Queue<Task>)workItems).Peek():null;
        }

        public string GetWorkItemQueueStatus(IEnumerable workItems)
        {
            return string.Join(",", (Queue<Task>) workItems);
        }

        #endregion

    }
}
