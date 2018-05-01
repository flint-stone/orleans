using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Orleans.Runtime.Scheduler.Utility;

namespace Orleans.Runtime.Scheduler.PoliciedScheduler.SchedulingStrategies
{
    internal class TestSchedulingStrategy : ISchedulingStrategy
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
            if (Scheduler.GetWorkItemGroup(workItem.SchedulingContext)!=null ) return workItem.PriorityContext;
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

        public void OnWorkItemInsert(IWorkItem workItem, WorkItemGroup wig)
        {
            // Do the math
            /*
            if (--statCollectionCounter <= 0)
            {
                statCollectionCounter = 100;
                foreach (var kv in tenantStatCounters) tenantStatCounters[kv.Key].Enqueue(kv.Key.CollectStats());
                logger.Info($"Printing execution times in ticks: {string.Join("********************", tenantStatCounters.Select(x => x.Key.ToString() + ':' + String.Join(",", x.Value)))}");
            }
            */
            // Change quantum if required
            // Or insert signal item for priority change?
            // if()
        }

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
        #endregion

        #region WorkItemGroup
        public IEnumerable CreateWorkItemQueue()
        {
            var workItemDictionary = new SortedDictionary<double, Queue<Task>>();
            workItemDictionary[0.0] = new Queue<Task>();
            return workItemDictionary;
        }

        public void AddToWorkItemQueue(Task task, IEnumerable workItems, WorkItemGroup wig)
        {
            var workItemDictionary = workItems as SortedDictionary<double, Queue<Task>>;
            var priority = workItemDictionary.Count>0?workItemDictionary.Keys.First():0.0;
            var contextObj = task.AsyncState as PriorityContext;
            if (contextObj != null)
            {
                // TODO: FIX LATER
                priority = contextObj.Priority == 0.0 ? wig.PriorityContext : contextObj.Priority;
            }
            if (!workItemDictionary.ContainsKey(priority))
            {
                workItemDictionary.Add(priority, new Queue<Task>());
            }
            workItemDictionary[priority].Enqueue(task);
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
            var workItemDictionary = workItems as SortedDictionary<double, Queue<Task>>;
            foreach (var kv in workItemDictionary)
            {
                foreach (Task task in kv.Value)
                {
                    // Ignore all queued Tasks, so in case they are faulted they will not cause UnobservedException.
                    task.Ignore();
                }
                workItemDictionary[kv.Key].Clear();
                workItemDictionary.Remove(kv.Key);
            }
        }

        public Task GetNextTaskForExecution(IEnumerable workItems)
        {
            var workItemDictionary = workItems as SortedDictionary<double, Queue<Task>>;

            var queue = workItemDictionary.First().Value;
            if (queue.Count > 0)
            {
                return queue.Dequeue();
            }
           
            // finish current priority, break and take wig off the queue
            workItemDictionary.Remove(workItemDictionary.Keys.First());
            return null;     
        }

        public int CountWIGTasks(IEnumerable workItems)
        {
            return ((SortedDictionary<double, Queue<Task>>)workItems).Values.Select(x => x.Count).Sum();
        }

        public Task GetOldestTask(IEnumerable workItems)
        {
            var workItemDictionary = workItems as SortedDictionary<double, Queue<Task>>;
            return workItemDictionary.Values.Select(x => x.Count).Sum() >= 0
                ? workItemDictionary[workItemDictionary.Keys.First()].Peek()
                : null;
        }

        public string GetWorkItemQueueStatus(IEnumerable workItems)
        {
            return string.Join("|||",
                ((SortedDictionary<double, Queue<Task>>)workItems).Select(x =>
                    x.Key + ":" + string.Join(",",
                        x.Value.Select(y =>
                            {
                                var contextObj = y.AsyncState as PriorityContext;
                                return "<" + y.ToString() + "-" +
                                       (contextObj?.Priority.ToString() ?? "null") + ">";
                            }
                        ))));
        }

        #endregion
    }
}
