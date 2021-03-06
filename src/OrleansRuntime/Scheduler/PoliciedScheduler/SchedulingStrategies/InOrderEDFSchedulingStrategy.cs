﻿using Orleans.Runtime.Scheduler.SchedulerUtility;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Orleans.Runtime.Scheduler.PoliciedScheduler.SchedulingStrategies
{
    internal class InOrderEDFSchedulingStrategy : ISchedulingStrategy
    {
       private LoggerImpl _logger;

        #region Tenancies
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
            var workitemManager = wig.WorkItemManager as InOrderEDFWorkItemManager;
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

            var wim = wig.WorkItemManager as InOrderEDFWorkItemManager;
            wim.StatManager.GetDownstreamContext(downstreamActivation, downstreamContext);
        }

        public WorkItemGroup CreateWorkItemGroup(IOrleansTaskScheduler ots, ISchedulingContext context)
        {
            var wig = new WorkItemGroup(ots, context);
            wig.WorkItemManager = new InOrderEDFWorkItemManager(this, wig);
            if (context.ContextType.Equals(SchedulingContextType.Activation) && windowedKeys.Keys.Contains(((SchedulingContext) context).Activation.Grain.Key.N1))
            {
                var wim = wig.WorkItemManager as InOrderEDFWorkItemManager;
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
                var wim = addressToWIG[sendingActivationAddress].WorkItemManager as InOrderEDFWorkItemManager;
                return wim.StatManager.CheckForStatsUpdate(upstream);
            }
            return null;
        }


    }


    internal class InOrderEDFWorkItemManager : IWorkItemManager
    {
        private Queue<Task> workItems;
        private readonly LoggerImpl _logger;
        private readonly WorkItemGroup workItemGroup;
        private int statCollectionCounter = SchedulerConstants.MEASUREMENT_PERIOD_WORKITEM_COUNT;
        private long wid;

        internal ConcurrentBag<GrainId> UpstreamOpSet { get; set; } // upstream Ops, populated during initialization
        private ConcurrentDictionary<ActivationAddress, long> DownstreamOpToCost { get; set; } // downstream Ops, populated while downstream message flows back

        private ConcurrentDictionary<GrainId, Tuple<Dictionary<string, long>, long>> StatsUpdatesCollection { get; set; }

        public InOrderEDFSchedulingStrategy Strategy { get; set; }
        internal long DataflowSLA { get; set; }
        public StatisticsManager StatManager { get; set; }
        public bool WindowedGrain { get; set; }
        public long WindowSize { get; set; }

        public InOrderEDFWorkItemManager(ISchedulingStrategy strategy, WorkItemGroup wig)
        {
            Strategy = (InOrderEDFSchedulingStrategy)strategy;
            StatManager = new StatisticsManager(wig, ((PriorityBasedTaskScheduler)Strategy.Scheduler).Metrics);
            workItems = new Queue<Task>();
            UpstreamOpSet = new ConcurrentBag<GrainId>();
            DownstreamOpToCost = new ConcurrentDictionary<ActivationAddress, long>();
            StatsUpdatesCollection = new ConcurrentDictionary<GrainId, Tuple<Dictionary<string, long>, long>>();
            DataflowSLA = SchedulerConstants.DEFAULT_DATAFLOW_SLA;
            _logger = LogManager.GetLogger(this.GetType().FullName, LoggerType.Runtime);
            workItemGroup = wig;
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
                // Setting priority of the wig
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

                
#if PQ_DEBUG
                _logger.Info($"{System.Reflection.MethodBase.GetCurrentMethod().Name} {workItemGroup}, {task} : {task.Id}, {timestamp} : {WindowSize}: {oldWid} -> {wid} : {DataflowSLA} : {maximumDownStreamPathCost} : {priority}");
#endif
                workItems.Enqueue(task);
                wig.PriorityContext = new PriorityObject(priority, Environment.TickCount);

            }
            else
            {
                var priority = wig.PriorityContext.Priority;
                
                //#if PQ_DEBUG
                _logger.Info($"{System.Reflection.MethodBase.GetCurrentMethod().Name} {workItemGroup}, {task} : {task.Id}, {priority}");
                //#endif
                workItems.Enqueue(task);
            }
#if PQ_DEBUG
            _logger.Info($"{System.Reflection.MethodBase.GetCurrentMethod().Name} {workItemGroup}, {GetWorkItemQueueStatus()}");
#endif
        }

        public void OnAddWIGToRunQueue(Task task, WorkItemGroup wig)
        {
            // var priority = workItems.Count > 0 ? workItems.Keys.First() : 0L;
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

            // wig.PriorityContext = new PriorityObject(priority, Environment.TickCount);
#if PQ_DEBUG
            _logger.Info($"OnAddWIGToRunQueue: {wig}:{wig.PriorityContext.Priority}:{wig.PriorityContext.Ticks}");
#endif

        }

        public void OnClosingWIG()
        {
            foreach (Task task in workItems)
            {
                // Ignore all queued Tasks, so in case they are faulted they will not cause UnobservedException.
                task.Ignore();
            }
            workItems.Clear();   
        }

        public Task GetNextTaskForExecution()
        {
            if (workItems.Any())
            {
                return workItems.Dequeue();
            }

            return null;
        }

        public void OnFinishingWIGTurn()
        {
            StatManager.UpdateWIGStatistics();
        }

        public int CountWIGTasks()
        {
            return workItems.Count;
        }

        public Task GetOldestTask()
        {
            return workItems.FirstOrDefault();
        }

        public string GetWorkItemQueueStatus()
        {
            return string.Join("|||", workItems);
        }

        public void OnReAddWIGToRunQueue(WorkItemGroup wig)
        {
            var priority = wig.PriorityContext.Priority;
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

        }

    }

}
