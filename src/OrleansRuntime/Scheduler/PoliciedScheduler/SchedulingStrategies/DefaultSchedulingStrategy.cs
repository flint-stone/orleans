using System;
using System.Collections.Generic;
using System.Linq;
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
            if (--statCollectionCounter <= 0)
            {
                statCollectionCounter = 100;
                foreach (var kv in tenantStatCounters) tenantStatCounters[kv.Key].Enqueue(kv.Key.CollectStats());
                logger.Info($"Printing execution times in ticks: {string.Join("********************", tenantStatCounters.Select(x => x.Key.ToString() + ':' + String.Join(",", x.Value)))}");
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
            if (!tenantStatCounters.ContainsKey(wig)) tenantStatCounters.Add(wig, new FixedSizedQueue<double>(MaximumStatCounterSize));
        }
    }
}
