using Orleans.Runtime.Scheduler.SchedulerUtility;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Orleans.Runtime.Scheduler.PoliciedScheduler.SchedulingStrategies
{
    internal class BoundaryBasedEDFSchedulingStrategy : ISchedulingStrategy
    {
       private LoggerImpl _logger;

        #region Tenancies

//        private ConcurrentDictionary<ActivationAddress, Dictionary<GrainId, Dictionary<string, long>>> StatsUpdatesCollection { get; set; }
//        // TODO: FIX! BAD DESIGN
//        private ConcurrentDictionary<ActivationAddress, long> DownstreamCostsCollection { get; set; }        
        // One downstream op maps to multiple WIGs in current scheduler
        public ConcurrentDictionary<ActivationAddress, HashSet<WorkItemGroup>> DownstreamOpsToWIGs { get; set; }  

        private Dictionary<ActivationAddress, WorkItemGroup> addressToWIG;
        private ConcurrentDictionary<ulong, long> windowedKeys;
        private int statCollectionCounter = SchedulerConstants.MEASUREMENT_PERIOD_WORKITEM_COUNT;

        #endregion

        public IOrleansTaskScheduler Scheduler { get; set; }

        public void Initialization()
        {
            _logger = LogManager.GetLogger(this.GetType().FullName, LoggerType.Runtime);
            DownstreamOpsToWIGs = new ConcurrentDictionary<ActivationAddress, HashSet<WorkItemGroup>>();
            addressToWIG = new Dictionary<ActivationAddress, WorkItemGroup>();
            windowedKeys = new ConcurrentDictionary<ulong, long>();
        }

        public void OnWorkItemInsert(IWorkItem workItem, WorkItemGroup wig){ }
        
        public void OnReceivingControllerInstructions(IWorkItem workItem, ISchedulingContext context)
        {
            // Populate Topology info
            var invokeWorkItem = workItem as InvokeWorkItem;
            var controllerContext = invokeWorkItem.ControllerContext;

            var schedulingContext = context as SchedulingContext;
            foreach (var k in controllerContext.windowedKey.Keys)
            {
                windowedKeys.AddOrUpdate(k, controllerContext.windowedKey[k], (key, value) => value);
#if PQ_DEBUG
                _logger.Info($"Add to windowedKeys Map {k} {controllerContext.windowedKey[k]}");
#endif
            }
            var wig = Scheduler.GetWorkItemGroup(schedulingContext);

            if (wig == null)
            {
                var error = string.Format(
                    "WorkItem {0} on context {1} does not match a work item group", workItem, context);
                _logger.Error(ErrorCode.SchedulerQueueWorkItemWrongCall, error);
                throw new InvalidOperationException(error);
            }
            
            // Populate info in wig
            var workitemManager = wig.WorkItemManager as PriorityBasedEDFWorkItemManager;
            workitemManager.DataflowSLA = (long) controllerContext.Time;

            if (windowedKeys.ContainsKey(((SchedulingContext) wig.SchedulingContext).Activation.Grain.Key.N1))
            {
                workitemManager.WindowedGrain = true;
                workitemManager.WindowSize =
                    windowedKeys[((SchedulingContext) wig.SchedulingContext).Activation.Grain.Key.N1];
            }

            // Populate upstream information
            if (!controllerContext.ActivationSeen.Contains(schedulingContext.Activation.Address))
            {
                // Leaves a footprint
                controllerContext.ActivationSeen.Add(schedulingContext.Activation.Address); 
                if (!workitemManager.UpstreamOpSet.Contains(invokeWorkItem.SourceActivation.Grain))
                    workitemManager.UpstreamOpSet.Add(invokeWorkItem.SourceActivation.Grain);
            }
            
        }

        public void OnReceivingDownstreamInstructions(IWorkItem workItem, ISchedulingContext context)
        {
            var invokeWorkItem = workItem as InvokeWorkItem;
            var downstreamContext = invokeWorkItem.DownstreamContext;
            var downstreamActivation = invokeWorkItem.SourceActivation;
            var schedulingContext = context as SchedulingContext;
            var wig = Scheduler.GetWorkItemGroup(schedulingContext);

            if (wig == null)
            {
                var error = string.Format(
                    "WorkItem {0} on context {1} does not match a work item group", workItem, context);
                _logger.Error(ErrorCode.SchedulerQueueWorkItemWrongCall, error);
                throw new InvalidOperationException(error);
            }

            var wim = wig.WorkItemManager as PriorityBasedEDFWorkItemManager;
            wim.GetDownstreamContext(downstreamActivation, downstreamContext);
        }

        public WorkItemGroup CreateWorkItemGroup(IOrleansTaskScheduler ots, ISchedulingContext context)
        {
            var wig = new WorkItemGroup(ots, context);
            wig.WorkItemManager = new PriorityBasedEDFWorkItemManager(this, wig);
            if (context.ContextType.Equals(SchedulingContextType.Activation) && windowedKeys.Keys.Contains(((SchedulingContext) context).Activation.Grain.Key.N1))
            {
                var wim = wig.WorkItemManager as PriorityBasedEDFWorkItemManager;
                wim.WindowedGrain = true;
                wim.WindowSize = windowedKeys[((SchedulingContext) context).Activation.Grain.Key.N1];
#if PQ_DEBUG
                _logger.Info($"{System.Reflection.MethodBase.GetCurrentMethod().Name} Windowed Key {((SchedulingContext)context).Activation.Grain.Key.N1} window size {wim.WindowSize}");
#endif
            }
            // populate addressToWIG for fast lookup
            if (context.ContextType.Equals(SchedulingContextType.Activation) && !addressToWIG.ContainsKey(((SchedulingContext)context).Activation.Address))
                addressToWIG[((SchedulingContext)context).Activation.Address] = wig;
            return wig;
        }

        public DownstreamContext CheckForSchedulerHint(ActivationAddress sendingActivationAddress, GrainId upstream)
        {
            if (addressToWIG.ContainsKey(sendingActivationAddress))
            {
                var wim = addressToWIG[sendingActivationAddress].WorkItemManager as PriorityBasedEDFWorkItemManager;
                return wim.CheckForStatsUpdate(upstream);
            }
            return null;
        }


        public long PeekNextDeadline()
        {
            var workItem = ((PriorityBasedTaskScheduler)Scheduler).NextInRunQueue();
            if (workItem != null)
            {
#if PQ_DEBUG
                _logger.Info($"{System.Reflection.MethodBase.GetCurrentMethod().Name} {workItem}");
#endif
                return workItem.PriorityContext.Priority;
            }
            return SchedulerConstants.DEFAULT_PRIORITY;
        }

    }


    internal class BoundaryBasedEDFWorkItemManager : IWorkItemManager
    {
        private SortedDictionary<long, Queue<Task>> workItems;
        private readonly LoggerImpl _logger;
        private readonly WorkItemGroup workItemGroup;
        private bool dequeuedFlag;
        private int statCollectionCounter = SchedulerConstants.MEASUREMENT_PERIOD_WORKITEM_COUNT;
        private long wid;

        internal ConcurrentBag<GrainId> UpstreamOpSet { get; set; } // upstream Ops, populated during initialization
        private ConcurrentDictionary<ActivationAddress, long> DownstreamOpToCost { get; set; } // downstream Ops, populated while downstream message flows back

        private ConcurrentDictionary<GrainId, Tuple<Dictionary<string, long>, long>> StatsUpdatesCollection { get; set; }

        public BoundaryBasedEDFSchedulingStrategy Strategy { get; set; }
        internal long DataflowSLA { get; set; }
        public bool WindowedGrain { get; set; }
        public long WindowSize { get; set; }

        public BoundaryBasedEDFWorkItemManager(ISchedulingStrategy strategy, WorkItemGroup wig)
        {
            Strategy = (BoundaryBasedEDFSchedulingStrategy)strategy;
            workItems = new SortedDictionary<long, Queue<Task>>();
            UpstreamOpSet = new ConcurrentBag<GrainId>();
            DownstreamOpToCost = new ConcurrentDictionary<ActivationAddress, long>();
            StatsUpdatesCollection = new ConcurrentDictionary<GrainId, Tuple<Dictionary<string, long>, long>>();
            DataflowSLA = SchedulerConstants.DEFAULT_DATAFLOW_SLA;
            _logger = LogManager.GetLogger(this.GetType().FullName, LoggerType.Runtime);
            workItemGroup = wig;
            dequeuedFlag = false;
        }

        public void AddToWorkItemQueue(Task task, WorkItemGroup wig)
        {
            var contextObj = task.AsyncState as PriorityContext;
            if (contextObj != null && contextObj.Timestamp != 0L)
            {
                // TODO: FIX LATER
                var timestamp = contextObj.Timestamp == SchedulerConstants.DEFAULT_PRIORITY ? wig.PriorityContext.Priority : contextObj.Timestamp;
#if PQ_DEBUG
                _logger.Info(
                    $"{System.Reflection.MethodBase.GetCurrentMethod().Name} {task}: {timestamp} {wig.PriorityContext}, {contextObj.Timestamp}");
#endif
                long maximumDownStreamPathCost = SchedulerConstants.DEFAULT_WIG_EXECUTION_COST;

                if (DownstreamOpToCost.Any()) maximumDownStreamPathCost = DownstreamOpToCost.Values.Max();

                var ownerStats = workItemGroup.WorkItemGroupStats; //(Dictionary<ActivationAddress, Dictionary<string, double>>)Strategy.FetchWorkItemMetric(workItemGroup);

#if PQ_DEBUG
                var source = contextObj.SourceActivation == null ? "null" : contextObj.SourceActivation.ToString();
                _logger.Info(
                    $"{System.Reflection.MethodBase.GetCurrentMethod().Name} {workItemGroup} {task} {maximumDownStreamPathCost} {source}: {string.Join(", ", ownerStats.Select(kv => kv.Key + ": " + string.Join("|", kv.Value.Select(sl => sl.Key + " -> " + sl.Value))))}");

#endif


                if (contextObj.SourceActivation != null && ownerStats.ContainsKey(contextObj.SourceActivation) && ownerStats[contextObj.SourceActivation].ContainsKey(task.ToString()))
                {
                    maximumDownStreamPathCost += Convert.ToInt64(ownerStats[contextObj.SourceActivation][task.ToString()]);
                }

                // ***
                // Setting priority of the task
                // ***
                var priority = timestamp + DataflowSLA - maximumDownStreamPathCost;
                var oldWid = wid;
                if (WindowedGrain)
                {
                    priority = (timestamp / WindowSize + 1) * WindowSize + DataflowSLA - maximumDownStreamPathCost;
                    if (timestamp / WindowSize > wid)
                    {
                        if (wid != 0L)
                            priority = (wid + 1) * WindowSize + DataflowSLA - maximumDownStreamPathCost;
                        wid = timestamp / WindowSize;
                    }
                }

                if (!workItems.ContainsKey(priority))
                {
                    workItems.Add(priority, new Queue<Task>());
#if PQ_DEBUG
                    _logger.Info($"{System.Reflection.MethodBase.GetCurrentMethod().Name} {workItemGroup} Adding priority, {priority}");
#endif
                }
#if PQ_DEBUG
                _logger.Info($"{System.Reflection.MethodBase.GetCurrentMethod().Name} {workItemGroup}, {task} : {task.Id}, {timestamp} : {WindowSize}: {oldWid} -> {wid} : {DataflowSLA} : {maximumDownStreamPathCost} : {priority}");
#endif
                workItems[priority].Enqueue(task);

            }
            else
            {
                var priority = workItems.Any() ? workItems.Keys.First() : wig.PriorityContext.Priority;
                if (!workItems.ContainsKey(priority))
                {
#if PQ_DEBUG
                _logger.Info($"{System.Reflection.MethodBase.GetCurrentMethod().Name} {workItemGroup} Adding priority, {priority}");
#endif
                    workItems.Add(priority, new Queue<Task>());
                }
#if PQ_DEBUG
                _logger.Info($"{System.Reflection.MethodBase.GetCurrentMethod().Name} {workItemGroup}, {task} : {task.Id}, {priority}");
#endif
                workItems[priority].Enqueue(task);
            }
#if PQ_DEBUG
            _logger.Info($"{System.Reflection.MethodBase.GetCurrentMethod().Name} {workItemGroup}, {GetWorkItemQueueStatus()}");
#endif
        }

        public void OnAddWIGToRunQueue(Task task, WorkItemGroup wig)
        {
            var priority = workItems.Count > 0 ? workItems.Keys.First() : 0L;
#if PQ_DEBUG
            _logger.Info(
                $"workitem queue: {string.Join(",", workItems.Keys)}");
            if (wig.PriorityContext <= priority)
            {
                _logger.Info(
                    $"{System.Reflection.MethodBase.GetCurrentMethod().Name} {task}: {wig.PriorityContext} <= {priority}");
            }
            else
            {
                _logger.Info(
                    $"{System.Reflection.MethodBase.GetCurrentMethod().Name} {task}: {wig.PriorityContext} > {priority}");
            }
#endif

            wig.PriorityContext = new PriorityObject(priority, Environment.TickCount);
#if PQ_DEBUG
            _logger.Info($"OnAddWIGToRunQueue: {wig}:{wig.PriorityContext.Priority}:{wig.PriorityContext.Ticks}");
#endif
            dequeuedFlag = true;
        }

        public void OnClosingWIG()
        {
            foreach (var kv in workItems.ToArray())
            {
                foreach (Task task in kv.Value.ToArray())
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
#if PQ_DEBUG
            _logger.Info($"Dequeue priority {kv.Key}");
#endif
            var nextDeadline = Strategy.PeekNextDeadline();

            if (workItems.Any() && workItems.First().Value.Any() && ((nextDeadline == SchedulerConstants.DEFAULT_PRIORITY || workItems.First().Key < nextDeadline) || dequeuedFlag))
            {
                var item = workItems.First().Value.Dequeue();
#if PQ_DEBUG
                _logger.Info($"{workItemGroup} Dequeue priority {workItems.First().Key} {item}");
#endif
                dequeuedFlag = false;
                // finish current priority, break and take wig off the queue
                if (!workItems.First().Value.Any())
                {
#if PQ_DEBUG
                    _logger.Info($"{System.Reflection.MethodBase.GetCurrentMethod().Name} {workItemGroup} Removing priority, {workItems.Keys.First()}");
#endif
                    workItems.Remove(workItems.Keys.First());
                }
                return item;
            }

            return null;
        }

        public void UpdateWIGStatistics()
        {
            if (--statCollectionCounter <= 0)
            {
                statCollectionCounter = SchedulerConstants.MEASUREMENT_PERIOD_WORKITEM_COUNT;
                workItemGroup.CollectStats();
                var statsToReport = workItemGroup.WorkItemGroupStats;
                foreach (var address in statsToReport.Keys)
                {
                    var statsUpdate = statsToReport[address];
                    var downstreamCost = DownstreamOpToCost.Values.Any()
                        ? DownstreamOpToCost.Values.Max()
                        : SchedulerConstants.DEFAULT_WIG_EXECUTION_COST;
#if PQ_DEBUG
                    _logger.Info($"{System.Reflection.MethodBase.GetCurrentMethod().Name} {workItemGroup.Name} -> {address} {string.Join(",", statsUpdate.Select(kv => kv.Key + "->" + kv.Value))} {downstreamCost}");
#endif
                    var tup = new Tuple<Dictionary<string, long>, long>(statsUpdate, downstreamCost);
                    StatsUpdatesCollection.AddOrUpdate(address.Grain, tup, (k, v) => tup);

                }
                // workItemGroup.LogExecTimeCounters();
            }
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
                                       (contextObj?.Timestamp.ToString() ?? "null") + ">";
                            }
                        ))));
        }

        public void OnReAddWIGToRunQueue(WorkItemGroup wig)
        {
            var priority = workItems.Count > 0 ? workItems.Keys.First() : SchedulerConstants.DEFAULT_PRIORITY;
#if PQ_DEBUG
            _logger.Info(
                $"workitem queue: {string.Join(",", workItems.Keys)}");
            if (wig.PriorityContext <= priority)
            {
                _logger.Info(
                    $"{System.Reflection.MethodBase.GetCurrentMethod().Name} {task}: {wig.PriorityContext} <= {priority}");
            }
            else
            {
                _logger.Info(
                    $"{System.Reflection.MethodBase.GetCurrentMethod().Name} {task}: {wig.PriorityContext} > {priority}");
            }
#endif

            wig.PriorityContext = new PriorityObject(priority, Environment.TickCount);
            dequeuedFlag = true;
        }


        public void GetDownstreamContext(ActivationAddress downstreamActivation, DownstreamContext downstreamContext)
        {
            // TODO: FIX LATER
#if PQ_DEBUG
                    _logger.Info($"{System.Reflection.MethodBase.GetCurrentMethod().Name} {workItemGroup} <- {downstreamActivation} {downstreamContext}");
#endif
            var maxDownstreamCost = downstreamContext.MaximumDownstreamCost +
                                    downstreamContext.ExecutionCostByTaskType.Count > 0 ? downstreamContext.ExecutionCostByTaskType.Values.Max() : SchedulerConstants.DEFAULT_WIG_EXECUTION_COST;
            DownstreamOpToCost.AddOrUpdate(downstreamActivation, maxDownstreamCost, (k, v) => maxDownstreamCost);
        }

        public DownstreamContext CheckForStatsUpdate(GrainId upstream)
        {
            Tuple<Dictionary<string, long>, long> tuple;
            if (StatsUpdatesCollection.TryGetValue(upstream, out tuple))
            {
                return new DownstreamContext(tuple.Item1, tuple.Item2);
            }
            return null;
        }
    }
}
