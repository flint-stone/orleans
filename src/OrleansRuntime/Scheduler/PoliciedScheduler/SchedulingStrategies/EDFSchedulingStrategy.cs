using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans.Runtime.Scheduler.Utility;

namespace Orleans.Runtime.Scheduler.PoliciedScheduler.SchedulingStrategies
{
    internal class EDFSchedulingStrategy : ISchedulingStrategy
    {
        private const double DEFAULT_PRIORITY = 0.0;
        private const double DEFAULT_WIG_EXEUCTION_COST = 0.0;
        private const int DEFAULT_TASK_QUANTUM_MILLIS = 100;
        private const int DEFAULT_TASK_QUANTUM_NUM_TASKS = 0;


        private readonly LoggerImpl logger = LogManager.GetLogger("Scheduler.PoliciedScheduler.SchedulingStrategies", LoggerType.Runtime);

        #region Tenancies

        private Dictionary<short, Tuple<ulong, HashSet<ulong>>> tenants;
        private Dictionary<short, long> timeLimitsOnTenants;
        private Dictionary<WorkItemGroup, double> tenantCostEstimate;
        private Dictionary<ActivationAddress, WorkItemGroup> addressToWIG;
        private const int MaximumStatCounterSize = 100;
        // TODO: FIX LATER
        private int statCollectionCounter = 100;

        #endregion

        public IOrleansTaskScheduler Scheduler { get; set; }

        #region ISchedulingStrategy
 
        public IComparable GetPriority(IWorkItem workItem)
        {
            if (Scheduler.GetWorkItemGroup(workItem.SchedulingContext) != null) return workItem.PriorityContext;
            return DEFAULT_PRIORITY;
        }

        public void Initialization()
        {
            tenants = new Dictionary<short, Tuple<ulong, HashSet<ulong>>>();
            timeLimitsOnTenants = new Dictionary<short, long>();
            tenantCostEstimate = new Dictionary<WorkItemGroup, double>();
            addressToWIG = new Dictionary<ActivationAddress, WorkItemGroup>();
        }

        public void OnWorkItemInsert(IWorkItem workItem, WorkItemGroup wig)
        {
            
            // Collect stat from WIGs
            if (tenantCostEstimate.Any() && --statCollectionCounter <= 0) 
            {
                statCollectionCounter = 100;
                // TODO: fix single level counter
                foreach (var kv in tenantCostEstimate) tenantCostEstimate[kv.Key] = kv.Key.CollectStats();
                logger.Info($"Printing execution times in ticks: {string.Join("********************", tenantCostEstimate.Select(x => x.Key.ToString() + ':' + x.Value))}");
            }
            
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
            // populate addressToWIG for fast lookup
            if (!addressToWIG.ContainsKey(((SchedulingContext)wig.SchedulingContext).Activation.Address))
                addressToWIG[((SchedulingContext)wig.SchedulingContext).Activation.Address] = wig;
            if (!tenantCostEstimate.ContainsKey(wig)) tenantCostEstimate.Add(wig, DEFAULT_WIG_EXEUCTION_COST);
            PopulateDependencyUpstream(((InvokeWorkItem) workItem).SourceActivation, wig, wig);
        }

        private void PopulateDependencyUpstream(ActivationAddress sourceActivation, WorkItemGroup wig, WorkItemGroup toAdd)
        {
            if (sourceActivation == null) return;
            WorkItemGroup upstreamWig;
            if(!addressToWIG.TryGetValue(sourceActivation, out upstreamWig))
            {
                var error = string.Format(
                    "Activation Address to WIG does not return a valid wig for activation {0}", sourceActivation);
                logger.Error(ErrorCode.SchedulerQueueWorkItemWrongCall, error);
                throw new InvalidOperationException(error);
            }
            ((EDFWorkItemManager)wig.WorkItemManager).UpstreamGroups.Add(upstreamWig);
            var paths = ((EDFWorkItemManager) upstreamWig.WorkItemManager).DownStreamPaths;
            foreach (var path in paths)
            {
                var pre = path.Peek();
                if (pre.Equals(wig)) path.Push(toAdd);
                PopulateDependencyUpstream(((SchedulingContext)pre.SchedulingContext).Activation.Address, upstreamWig, toAdd);
            }
            
        }

        public WorkItemGroup CreateWorkItemGroup(IOrleansTaskScheduler ots, ISchedulingContext context)
        {
            var wig = new WorkItemGroup(ots, context);
            wig.WorkItemManager = new EDFWorkItemManager();
            return wig;
        }

        #endregion

    }

    internal class EDFWorkItemManager : IWorkItemManager
    {
        private SortedDictionary<double, Queue<Task>> workItems;
        internal  List<WorkItemGroup> UpstreamGroups { get; set; } // upstream WIGs groups for backtracking
        internal List<Stack<WorkItemGroup>> DownStreamPaths { get; set; } // downstream WIG paths groups for calculation

        public EDFWorkItemManager()
        {
            workItems = new SortedDictionary<double, Queue<Task>>();
            UpstreamGroups = new List<WorkItemGroup>();
            DownStreamPaths = new List<Stack<WorkItemGroup>>();
        }

        public void AddToWorkItemQueue(Task task, WorkItemGroup wig)
        {
            var priority = workItems.Count > 0 ? workItems.Keys.First() : 0.0;
            var contextObj = task.AsyncState as PriorityContext;
            if (contextObj != null)
            {
                // TODO: FIX LATER
                priority = contextObj.Priority == 0.0 ? wig.PriorityContext : contextObj.Priority;
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
            var priority = contextObj?.Priority ?? 0.0;
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
                                   (contextObj?.Priority.ToString() ?? "null") + ">";
                        }
                        ))));
        }

        public void OnReAddWIGToRunQueue() { }
    }
}
