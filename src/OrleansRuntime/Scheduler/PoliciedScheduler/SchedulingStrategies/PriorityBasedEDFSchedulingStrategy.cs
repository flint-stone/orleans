﻿#define EDF_TRACKING


using Orleans.Runtime.Scheduler.SchedulerUtility;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace Orleans.Runtime.Scheduler.PoliciedScheduler.SchedulingStrategies
{
    internal class PriorityBasedEDFSchedulingStrategy : ISchedulingStrategy
    {
        private LoggerImpl _logger;
        private Dictionary<ActivationAddress, WorkItemGroup> addressToWIG;
        private ConcurrentDictionary<ulong, long> windowedKeys;

        public IOrleansTaskScheduler Scheduler { get; set; }

        public void Initialization()
        {
            _logger = LogManager.GetLogger(this.GetType().FullName, LoggerType.Runtime);
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
            var workitemManager = wig.WorkItemManager as TSBasedEDFWorkItemManager;
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
                if (!workitemManager.StatManager.UpstreamOpSet.Contains(invokeWorkItem.SourceActivation.Grain))
                    workitemManager.StatManager.UpstreamOpSet.Add(invokeWorkItem.SourceActivation.Grain);
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

            var wim = wig.WorkItemManager as TSBasedEDFWorkItemManager;
            wim.StatManager.GetDownstreamContext(downstreamActivation, downstreamContext);
        }

        public WorkItemGroup CreateWorkItemGroup(IOrleansTaskScheduler ots, ISchedulingContext context)
        {
            var wig = new WorkItemGroup(ots, context);
            wig.WorkItemManager = new TSBasedEDFWorkItemManager(this, wig);
            if (context.ContextType.Equals(SchedulingContextType.Activation) && windowedKeys.Keys.Contains(((SchedulingContext) context).Activation.Grain.Key.N1))
            {
                var wim = wig.WorkItemManager as TSBasedEDFWorkItemManager;
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
                var wim = addressToWIG[sendingActivationAddress].WorkItemManager as TSBasedEDFWorkItemManager;
                return wim.StatManager.CheckForStatsUpdate(upstream);
            }
            return null;
        }


        public long PeekNextDeadline()
        {
            var workItem = ((PriorityBasedTaskScheduler)Scheduler).NextInRunQueue();
            if (workItem != null && workItem.GetType() == typeof(WorkItemGroup))
            {
#if PQ_DEBUG
                _logger.Info($"{System.Reflection.MethodBase.GetCurrentMethod().Name} {workItem}");
#endif
                var wim = ((WorkItemGroup) workItem).WorkItemManager as TSBasedEDFWorkItemManager;
                return wim.PeekNextDeadline();
            }
            return SchedulerConstants.DEFAULT_PRIORITY;
        }

    }
   
    
        
    internal class PriorityBasedEDFWorkItemManager : IWorkItemManager
    {
        private readonly PriorityBasedEDFSchedulingStrategy strategy;
        private readonly SortedDictionary<long, Queue<Task>> workItems;
        private readonly LoggerImpl _logger; 
        private readonly WorkItemGroup workItemGroup;
        private bool dequeuedFlag;
        private long wid;


        internal StatisticsManager StatManager { get; }
        internal long DataflowSLA { get; set; }
        public bool WindowedGrain { get; set; }
        public long WindowSize { get; set; }

        public PriorityBasedEDFWorkItemManager(ISchedulingStrategy iSchedulingStrategy, WorkItemGroup wig)
        {
            strategy = (PriorityBasedEDFSchedulingStrategy)iSchedulingStrategy; 
            workItems = new SortedDictionary<long, Queue<Task>>();
            _logger = LogManager.GetLogger(this.GetType().FullName, LoggerType.Runtime);
            workItemGroup = wig;
            dequeuedFlag = false;
            wid = 0L;

            StatManager = new StatisticsManager(wig, ((PriorityBasedTaskScheduler)this.strategy.Scheduler).Metrics);
            DataflowSLA = SchedulerConstants.DEFAULT_DATAFLOW_SLA;
        }

        public void AddToWorkItemQueue(Task task, WorkItemGroup wig)
        {
            var contextObj = task.AsyncState as PriorityContext;
            if (contextObj != null && contextObj.Priority != SchedulerConstants.DEFAULT_PRIORITY)
            {
                var timestamp = contextObj.Priority == SchedulerConstants.DEFAULT_PRIORITY ? wig.PriorityContext.Priority : contextObj.Priority;
#if PQ_DEBUG
                _logger.Info(
                    $"{System.Reflection.MethodBase.GetCurrentMethod().Name} {task}: {timestamp} {wig.PriorityContext}, {contextObj.Timestamp}");
#endif
                long maximumDownStreamPathCost = SchedulerConstants.DEFAULT_WIG_EXECUTION_COST;

                if (StatManager.DownstreamOpToCost.Any()) maximumDownStreamPathCost = StatManager.DownstreamOpToCost.Values.Max();

                var ownerStats = workItemGroup.WorkItemGroupStats; //(Dictionary<ActivationAddress, Dictionary<string, double>>)Strategy.FetchWorkItemMetric(workItemGroup);
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
                        if(wid!=0L)
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
            _logger.Info($"{System.Reflection.MethodBase.GetCurrentMethod().Name} {workItemGroup}, {task}, {timestamp} : {WindowSize}: {oldWid} -> {wid} : {DataflowSLA} : {maximumDownStreamPathCost} : {priority}");
#endif
                workItems[priority].Enqueue(task);

            }
            else
            {
                var priority = workItems.Any()?workItems.Keys.First():wig.PriorityContext.Priority;
                if (!workItems.ContainsKey(priority))
                {
#if PQ_DEBUG
                _logger.Info($"{System.Reflection.MethodBase.GetCurrentMethod().Name} {workItemGroup} Adding priority, {priority}");
#endif
                    workItems.Add(priority, new Queue<Task>());
                }
#if PQ_DEBUG
                _logger.Info($"{System.Reflection.MethodBase.GetCurrentMethod().Name} {workItemGroup}, {task}, {priority}");
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

            wig.PriorityContext = new PriorityObject(priority, Environment.TickCount );
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
            var nextDeadline = strategy.PeekNextDeadline();
            
            if (workItems.Any() && workItems.First().Value.Any() && ((nextDeadline == SchedulerConstants.DEFAULT_PRIORITY || workItems.First().Key < nextDeadline) || dequeuedFlag ))
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

        public void OnFinishingWIGTurn()
        {
            StatManager.UpdateWIGStatistics();
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

        public long PeekNextDeadline()
        {
            return workItemGroup.PriorityContext.Priority;
        }
    }


    internal class TSBasedEDFWorkItemManager : IWorkItemManager
    {
        private readonly PriorityBasedEDFSchedulingStrategy strategy;
        private readonly SortedDictionary<long, Queue<Task>> workItems;
        private readonly SortedDictionary<long, long> timestampsToDeadlines;
        private readonly LoggerImpl _logger;
        private readonly WorkItemGroup workItemGroup;
        private bool dequeuedFlag;
        private long wid;
               
#if EDF_TRACKING
        private int currentlyTracking;
        private FixedSizedQueue<long> queuingDelays;
        private readonly Stopwatch stopwatch;
#endif
        internal long DataflowSLA { get; set; }

        
        public StatisticsManager StatManager { get; }   
        public bool WindowedGrain { get; set; }
        public long WindowSize { get; set; }

        public TSBasedEDFWorkItemManager(ISchedulingStrategy iSchedulingStrategy, WorkItemGroup wig)
        {
            strategy = (PriorityBasedEDFSchedulingStrategy)iSchedulingStrategy;      
            workItems = new SortedDictionary<long, Queue<Task>>();
            timestampsToDeadlines = new SortedDictionary<long, long>();
            _logger = LogManager.GetLogger(this.GetType().FullName, LoggerType.Runtime);
            workItemGroup = wig;
            dequeuedFlag = false;
            wid = 0L;
#if EDF_TRACKING
            currentlyTracking = SchedulerConstants.DEFAULT_TASK_TRACKING_ID;
            queuingDelays = new FixedSizedQueue<long>(SchedulerConstants.STATS_COUNTER_QUEUE_SIZE);
            stopwatch = new Stopwatch();
#endif
            DataflowSLA = SchedulerConstants.DEFAULT_DATAFLOW_SLA;
            StatManager = new StatisticsManager(wig, ((PriorityBasedTaskScheduler)strategy.Scheduler).Metrics);
        }

        public void AddToWorkItemQueue(Task task, WorkItemGroup wig)
        {
            var contextObj = task.AsyncState as PriorityContext;
            var timestamp = contextObj?.Priority ?? SchedulerConstants.DEFAULT_PRIORITY;
            if (!workItems.ContainsKey(timestamp))
            {
                workItems.Add(timestamp, new Queue<Task>());

                
                if(!timestampsToDeadlines.ContainsKey(timestamp))
                {
                    if (timestamp != SchedulerConstants.DEFAULT_PRIORITY)
                    {
                        long maximumDownStreamPathCost = SchedulerConstants.DEFAULT_WIG_EXECUTION_COST;

                        if (StatManager.DownstreamOpToCost.Any()) maximumDownStreamPathCost = StatManager.DownstreamOpToCost.Values.Max();

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
                        var oldWid = wid;
                        if (WindowedGrain)
                        {
                            deadline = (timestamp / WindowSize + 1) * WindowSize + DataflowSLA -
                                       maximumDownStreamPathCost;
                            if (timestamp / WindowSize > wid)
                            {
                                if (wid != 0L)
                                    deadline = (wid + 1) * WindowSize + DataflowSLA - maximumDownStreamPathCost;
                                wid = timestamp / WindowSize;
                            }
                        }

                        timestampsToDeadlines.Add(timestamp, deadline);
                        // If timestamp is not default and earlier than the earliest non-zero deadline, update deadline accordingly
                        // if (timestamp != SchedulerConstants.DEFAULT_PRIORITY && workItems.Count > 1 && timestamp < workItems.Keys.ElementAt(1)) wig.PriorityContext.Deadline = deadline;
                    }
                    else
                    {
                        timestampsToDeadlines.Add(timestamp, SchedulerConstants.DEFAULT_PRIORITY);
                    }   
#if PQ_DEBUG
            _logger.Info($"{System.Reflection.MethodBase.GetCurrentMethod().Name} {workItemGroup}, {task}, {timestamp} : {WindowSize}: {oldWid} -> {wid} : {DataflowSLA} : {maximumDownStreamPathCost} : {deadline}");
#endif
                }
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

            var nextDeadline = strategy.PeekNextDeadline();
            if (workItems.Count>0 && (nextDeadline == SchedulerConstants.DEFAULT_PRIORITY || timestampsToDeadlines[workItems.First().Key] <= nextDeadline || dequeuedFlag))
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

                if (workItems.First().Value.Count==0)
                {
#if PQ_DEBUG
                    _logger.Info($"{System.Reflection.MethodBase.GetCurrentMethod().Name} {workItemGroup} Removing priority, {workItems.Keys.First()}");
#endif
                    timestampsToDeadlines.Remove(workItems.Keys.First());
                    workItems.Remove(workItems.Keys.First());
                }
                return item;
            }
            return null;
        }

        public void OnFinishingWIGTurn()
        {
            StatManager.UpdateWIGStatistics();
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
                    "Deadline " + timestampsToDeadlines[x.Key] + " "+
                     string.Join(",",
                        x.Value.Select(y =>
                            {

                                var contextObj = y.AsyncState as PriorityContext;
                                return "<" + y.ToString() + "-" +
                                        (contextObj?.Priority.ToString() ?? "null") + "-"
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

        public long PeekNextDeadline()
        {
            lock (workItemGroup.lockable)
            {
                var tses = timestampsToDeadlines.Keys;
                if (tses.Count != 0)
                {
                    if (tses.First() != SchedulerConstants.DEFAULT_PRIORITY) return tses.First();
                    if (tses.Count > 1) return tses.ElementAt(1);
                }
                return SchedulerConstants.DEFAULT_PRIORITY;
            }
        }
    }
}
