﻿using Orleans.Runtime.Scheduler.SchedulerUtility;
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
            var workitemManager = wig.WorkItemManager as BoundaryBasedEDFWorkItemManager;
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

            var wim = wig.WorkItemManager as BoundaryBasedEDFWorkItemManager;
            wim.GetDownstreamContext(downstreamActivation, downstreamContext);
        }

        public WorkItemGroup CreateWorkItemGroup(IOrleansTaskScheduler ots, ISchedulingContext context)
        {
            var wig = new WorkItemGroup(ots, context);
            wig.WorkItemManager = new BoundaryBasedEDFWorkItemManager(this, wig);
            if (context.ContextType.Equals(SchedulingContextType.Activation) && windowedKeys.Keys.Contains(((SchedulingContext) context).Activation.Grain.Key.N1))
            {
                var wim = wig.WorkItemManager as BoundaryBasedEDFWorkItemManager;
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
                var wim = addressToWIG[sendingActivationAddress].WorkItemManager as BoundaryBasedEDFWorkItemManager;
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
        private readonly SortedDictionary<long, Queue<Task>> workItems;
        private readonly SortedDictionary<long, long> timestampsToDeadlines;
        private readonly LoggerImpl _logger;
        private readonly WorkItemGroup workItemGroup;
        private bool dequeuedFlag;
        private int statCollectionCounter = SchedulerConstants.MEASUREMENT_PERIOD_WORKITEM_COUNT;
        private long wid;

#if EDF_TRACKING
        private int currentlyTracking;
        private FixedSizedQueue<long> queuingDelays;
        private readonly Stopwatch stopwatch;
#endif

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
            timestampsToDeadlines = new SortedDictionary<long, long>();
            UpstreamOpSet = new ConcurrentBag<GrainId>();
            DownstreamOpToCost = new ConcurrentDictionary<ActivationAddress, long>();
            StatsUpdatesCollection = new ConcurrentDictionary<GrainId, Tuple<Dictionary<string, long>, long>>();
            DataflowSLA = SchedulerConstants.DEFAULT_DATAFLOW_SLA;
            _logger = LogManager.GetLogger(this.GetType().FullName, LoggerType.Runtime);
            workItemGroup = wig;
            dequeuedFlag = false;
#if EDF_TRACKING
            currentlyTracking = SchedulerConstants.DEFAULT_TASK_TRACKING_ID;
            queuingDelays = new FixedSizedQueue<long>(SchedulerConstants.STATS_COUNTER_QUEUE_SIZE);
            stopwatch = new Stopwatch();
#endif
        }

        public void AddToWorkItemQueue(Task task, WorkItemGroup wig)
        {
            var contextObj = task.AsyncState as PriorityContext;
            var timestamp = contextObj?.Timestamp ?? SchedulerConstants.DEFAULT_PRIORITY;
            var oldWid = wid;
            if (WindowedGrain && timestamp!=SchedulerConstants.DEFAULT_PRIORITY)
            {
                timestamp = (timestamp / WindowSize + 1) * WindowSize;
                if (timestamp / WindowSize > wid)
                {
                    if (wid != 0L)
                        timestamp = (wid + 1) * WindowSize;
                    wid = timestamp / WindowSize;
                }
            }

            if (timestamp == SchedulerConstants.DEFAULT_PRIORITY)
            {
                timestamp = workItems.Any()?
                    (workItems.Count==1 || (workItems.Keys.First()!=SchedulerConstants.DEFAULT_PRIORITY)?workItems.Keys.First():workItems.Keys.ElementAt(1))
                    :SchedulerConstants.DEFAULT_PRIORITY;
            }

            long maximumDownStreamPathCost=0L;
            if (!workItems.ContainsKey(timestamp))
            {
                if (timestamp != SchedulerConstants.DEFAULT_PRIORITY)
                {
                    maximumDownStreamPathCost = SchedulerConstants.DEFAULT_WIG_EXECUTION_COST;

                    if (DownstreamOpToCost.Any()) maximumDownStreamPathCost = DownstreamOpToCost.Values.Max();

                    var ownerStats = workItemGroup.WorkItemGroupStats;
                    if (contextObj.SourceActivation != null &&
                        ownerStats.ContainsKey(contextObj.SourceActivation) &&
                        ownerStats[contextObj.SourceActivation].ContainsKey(task.ToString()))
                    {
                        maximumDownStreamPathCost +=
                            Convert.ToInt64(ownerStats[contextObj.SourceActivation][task.ToString()]);
                    }

                    // ***
                    // Setting priority of the task
                    // ***
                    var deadline = timestamp + DataflowSLA - maximumDownStreamPathCost;                       

                    timestampsToDeadlines.Add(timestamp, deadline);
                    // If timestamp is not default and earlier than the earliest non-zero deadline, update deadline accordingly
                    // if (timestamp != SchedulerConstants.DEFAULT_PRIORITY && workItems.Count > 1 && timestamp < workItems.Keys.ElementAt(1)) wig.PriorityContext.Deadline = deadline;
                }
                else
                {
                    timestampsToDeadlines.Add(timestamp, SchedulerConstants.DEFAULT_PRIORITY);
                }
#if PQ_DEBUG
                _logger.Info($"{System.Reflection.MethodBase.GetCurrentMethod().Name} {workItemGroup}, {task}, {timestamp} : {WindowSize}: {oldWid} -> {wid} : {DataflowSLA} : {maximumDownStreamPathCost} : {timestampsToDeadlines[timestamp]}");
#endif
                workItems.Add(timestamp, new Queue<Task>());
            }
            workItems[timestamp].Enqueue(task);
#if EDF_TRACKING
            if (currentlyTracking == SchedulerConstants.DEFAULT_TASK_TRACKING_ID)
            {
                currentlyTracking = task.Id;
                stopwatch.Start();
            }
            _logger.Info($"{string.Join(",", queuingDelays)}");
#endif
#if PQ_DEBUG
            _logger.Info($"{System.Reflection.MethodBase.GetCurrentMethod().Name} {workItemGroup}"+ 
                Environment.NewLine+
                "WorkItemQueueStatus: "+
                Environment.NewLine+
                $"{GetWorkItemQueueStatus()}");
#endif
        }

        public void OnAddWIGToRunQueue(Task task, WorkItemGroup wig)
        {
            var priority = PeekNextDeadline();
#if PQ_DEBUG
            _logger.Info($"OnAddWIGToRunQueue  {wig}  {GetWorkItemQueueStatus()}");
            if (wig.PriorityContext.Priority <= priority)
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

            if (workItems.First().Value.Count == 0)
            {
#if PQ_DEBUG
                _logger.Info($"{System.Reflection.MethodBase.GetCurrentMethod().Name} {workItemGroup} Removing priority, {workItems.Keys.First()}");
#endif
                timestampsToDeadlines.Remove(workItems.Keys.First());
                workItems.Remove(workItems.Keys.First());
            }

            if (workItems.Count > 0 && (nextDeadline == SchedulerConstants.DEFAULT_PRIORITY || timestampsToDeadlines[workItems.First().Key] <= nextDeadline || dequeuedFlag))
            //if (workItems.Any())
            {
                var item = workItems.First().Value.Dequeue();
                dequeuedFlag = false;

#if EDF_TRACKING
                if (item.Id == currentlyTracking)
                {
                    var elapsed = stopwatch.Elapsed.Ticks;
                    queuingDelays.Enqueue(elapsed);
                    currentlyTracking = SchedulerConstants.DEFAULT_TASK_TRACKING_ID;
                    stopwatch.Stop();
                }              
#endif
                //_logger.Info($"Dequeue item {item} with id {item.Id}");
                return item;
            }
            //_logger.Info("Dequeue WIG");
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
            return string.Join(Environment.NewLine,
                workItems.Select(x =>
                    "~~" + x.Key + ":" +
                    "Deadline " + timestampsToDeadlines[x.Key] + " " +
                    string.Join(",",
                        x.Value.Select(y =>
                            {

                                var contextObj = y.AsyncState as PriorityContext;
                                return "<" + y.ToString() + "-" +
                                       (contextObj?.Timestamp.ToString() ?? "null") + "-"
                                       + y.Id +
                                       ">";
                            }
                        ))));
        }

        public void OnReAddWIGToRunQueue(WorkItemGroup wig)
        {
            var priority = PeekNextDeadline();
#if PQ_DEBUG
            _logger.Info($"OnReAddWIGToRunQueue {wig}  {GetWorkItemQueueStatus()}");
            if (wig.PriorityContext.Priority <= priority)
            {
                _logger.Info(
                    $"{System.Reflection.MethodBase.GetCurrentMethod().Name}: {wig.PriorityContext} <= {priority}");
            }
            else
            {
                _logger.Info(
                    $"{System.Reflection.MethodBase.GetCurrentMethod().Name}: {wig.PriorityContext} > {priority}");
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

        public long PeekNextDeadline()
        {
            lock (workItemGroup.lockable)
            {
                return timestampsToDeadlines.Values.First();
            }
        }
    }
}
