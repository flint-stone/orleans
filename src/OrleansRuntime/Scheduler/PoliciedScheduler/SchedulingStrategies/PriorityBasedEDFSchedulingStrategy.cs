using Orleans.Runtime.Scheduler.SchedulerUtility;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Orleans.Runtime.Scheduler.PoliciedScheduler.SchedulingStrategies
{
    internal class PriorityBasedEDFSchedulingStrategy : ISchedulingStrategy
    {
       private LoggerImpl _logger;

        #region Tenancies
        public ConcurrentDictionary<WorkItemGroup, Dictionary<ActivationAddress, Dictionary<string, double>>> TenantCostEstimate { get; set; }

        private Dictionary<ActivationAddress, WorkItemGroup> addressToWIG;
        private ConcurrentDictionary<ulong, long> windowedKeys;
        private int statCollectionCounter = SchedulerConstants.MEASUREMENT_PERIOD_WORKITEM_COUNT;

        #endregion

        public IOrleansTaskScheduler Scheduler { get; set; }

        public void Initialization()
        {
            _logger = LogManager.GetLogger(this.GetType().FullName, LoggerType.Runtime);
            TenantCostEstimate = new ConcurrentDictionary<WorkItemGroup, Dictionary<ActivationAddress, Dictionary<string, double>>>();
            addressToWIG = new Dictionary<ActivationAddress, WorkItemGroup>();
            windowedKeys = new ConcurrentDictionary<ulong, long>();
        }

        public void OnWorkItemInsert(IWorkItem workItem, WorkItemGroup wig)
        {
            // Collect stat from WIGs
            if (TenantCostEstimate.Any() && --statCollectionCounter <= 0) 
            {
                statCollectionCounter = SchedulerConstants.MEASUREMENT_PERIOD_WORKITEM_COUNT;
#if PQ_DEBUG
                _logger.Info($"Printing execution times in ticks: {string.Join("********************", TenantCostEstimate.Select(x => x.Key.ToString() + " => " +string.Join("|", x.Value.Select(ad => ad.Key + ":" + string.Join(",", ad.Value.Select(kd=>kd.Key + " -> " + kd.Value))))))}");
#endif
            }
        }
        
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

            if (!TenantCostEstimate.ContainsKey(wig)) TenantCostEstimate.TryAdd(wig, new Dictionary<ActivationAddress, Dictionary<string, double>>());

            WorkItemGroup upstreamWig;
            if (!addressToWIG.TryGetValue(invokeWorkItem.SourceActivation, out upstreamWig))
            {
                var error = string.Format(
                    "Activation Address to WIG does not return a valid wig for activation {0}", invokeWorkItem.SourceActivation);
                _logger.Error(ErrorCode.SchedulerQueueWorkItemWrongCall, error);
                throw new InvalidOperationException(error);
            }
            // Remove cyclic invokes
            if (!workitemManager.UpstreamGroups.Contains(wig) && upstreamWig!= wig)
            {
                workitemManager.UpstreamGroups.Add(upstreamWig);
            }
            if (schedulingContext.Activation.Grain.Key.N1 == controllerContext.ControllerKey) return;

            PopulateDependencyUpstream(upstreamWig, wig, wig);

        }
        
        private void PopulateDependencyUpstream(WorkItemGroup upstreamWig, WorkItemGroup wig, WorkItemGroup toAdd)
        {
            if (upstreamWig.Equals(toAdd)) return; //remove self-invokation

            var workItemManager = upstreamWig.WorkItemManager as PriorityBasedEDFWorkItemManager;
            var paths = workItemManager.DownStreamPaths;
            if (workItemManager.UpstreamGroups.Contains(wig) || workItemManager.UpstreamGroups.Contains(toAdd)) return;
            bool found = false;
            foreach (var path in paths)
            {
                var pre = path.Peek();
                if (pre.Equals(wig) )
                {
                    path.Push(toAdd); // acyclic
                    found = true;
                }             
            }
            if (!found)
            {
                // no path found
                var newPath = new Stack<WorkItemGroup>();
                newPath.Push(toAdd);
                paths.Add(newPath);
            }
            foreach(var upstream in workItemManager.UpstreamGroups) PopulateDependencyUpstream(upstream, upstreamWig, toAdd);
            Console.WriteLine("Current upstreamWIG " + upstreamWig + ": " + workItemManager.ExplainDependencies()); 
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

        public object FetchWorkItemMetric(WorkItemGroup workItem)
        {
            return TenantCostEstimate.ContainsKey(workItem) ? TenantCostEstimate[workItem] : new Dictionary<ActivationAddress, Dictionary<string, double>>();
        }

        public void PutWorkItemMetric(WorkItemGroup workItemGroup, object metric)
        {
            if(TenantCostEstimate.ContainsKey(workItemGroup)) TenantCostEstimate[workItemGroup] = (Dictionary<ActivationAddress, Dictionary<string, double>>) metric;
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
    /*
    internal class PriorityBasedEDFWorkItemManager : IWorkItemManager
    {
        private const long DEFAULT_DATAFLOW_SLA = 5000000;
        private PriorityQueue<Tuple<long, Task>> workItems;
        private readonly LoggerImpl _logger; //  = LogManager.GetLogger("Scheduler.PoliciedScheduler.SchedulingStrategies", LoggerType.Runtime);
        private readonly WorkItemGroup workItemGroup;
        internal List<WorkItemGroup> UpstreamGroups { get; set; } // upstream WIGs groups for backtracking
        internal List<Stack<WorkItemGroup>> DownStreamPaths { get; set; } // downstream WIG paths groups for calculation
        internal ISchedulingStrategy Strategy { get; set; }
        internal long MaximumDownStreamPathCost { get; set; }
        internal long DataflowSLA { get; set; }

        public PriorityBasedEDFWorkItemManager(ISchedulingStrategy strategy, WorkItemGroup wig)
        {
            Strategy = strategy;
            workItems = new PriorityQueue<Tuple<long, Task>>(15, new TaskComparer());
            UpstreamGroups = new List<WorkItemGroup>();
            DownStreamPaths = new List<Stack<WorkItemGroup>>();
            MaximumDownStreamPathCost = 0L;
            DataflowSLA = DEFAULT_DATAFLOW_SLA;
            _logger = LogManager.GetLogger(this.GetType().FullName, LoggerType.Runtime);
            workItemGroup = wig;
        }

        public void AddToWorkItemQueue(Task task, WorkItemGroup wig)
        {
            //            var timestamp = workItems.Count > 0 ? workItems.Peek().Item1 : 0L;
            //            var contextObj = task.AsyncState as PriorityContext;
            //            if (contextObj != null)
            //            {
            //                // TODO: FIX LATER
            //                timestamp = contextObj.Timestamp == 0L ? wig.PriorityContext : contextObj.Timestamp;
            //#if DEBUG
            //                _logger.Info($"{System.Reflection.MethodBase.GetCurrentMethod().Name} {task}: {timestamp} {wig.PriorityContext}, {contextObj.Timestamp}");
            //#endif
            //            }
            //            
            //            foreach (var stack in DownStreamPaths)
            //            {
            //                var pathCost = 0L;
            //                // collect all metrics along the way
            //                foreach (var elem in stack)
            //                {
            //                    // TODO: fix later with window information
            //                    var cost = Strategy.FetchWorkItemMetric(elem);
            //                    pathCost += cost;
            //                }
            //                if (pathCost > MaximumDownStreamPathCost) MaximumDownStreamPathCost = pathCost;
            //            }
            //            // ***
            //            // Setting priority of the task
            //            // ***
            //            var priority = timestamp + DataflowSLA - MaximumDownStreamPathCost;
            //            
            //            workItems.Add(new Tuple<long, Task>(priority, task));
            //#if DEBUG
            //            _logger.Info($"{workItemGroup}, {task}, {timestamp}:{DataflowSLA}:{MaximumDownStreamPathCost}:{priority}");
            //            _logger.Info($"{workItemGroup}, {GetWorkItemQueueStatus()}");
            //#endif

            var contextObj = task.AsyncState as PriorityContext;
            if (contextObj != null && contextObj.Timestamp != 0L)
            {
                var timestamp = contextObj.Timestamp;
#if DEBUG
                _logger.Info(
                    $"{System.Reflection.MethodBase.GetCurrentMethod().Name} {task}: {timestamp} {wig.PriorityContext}, {contextObj.Timestamp}");
#endif
                foreach (var stack in DownStreamPaths)
                {
                    var pathCost = 0L;
                    // collect all metrics along the way
                    foreach (var elem in stack)
                    {
                        // TODO: fix later with window information
                        var cost = Strategy.FetchWorkItemMetric(elem);
                        pathCost += cost;
                    }
                    if (pathCost > MaximumDownStreamPathCost) MaximumDownStreamPathCost = pathCost;
                }
                // ***
                // Setting priority of the task
                // ***
                var priority = timestamp + DataflowSLA - MaximumDownStreamPathCost;

                workItems.Add(new Tuple<long, Task>(priority, task));
#if DEBUG
                _logger.Info($"{System.Reflection.MethodBase.GetCurrentMethod().Name} {workItemGroup}, {task}, {timestamp}:{DataflowSLA}:{MaximumDownStreamPathCost}:{priority}");
#endif
            }
            else
            {
                var priority = workItems.Any()?workItems.Peek().Item1: wig.PriorityContext;
                workItems.Add(new Tuple<long, Task>(priority, task));
#if DEBUG
                _logger.Info($"{System.Reflection.MethodBase.GetCurrentMethod().Name} {workItemGroup}, {task}, {priority}");
#endif
            }

#if DEBUG
              _logger.Info($"{workItemGroup}, {GetWorkItemQueueStatus()}");
//            if (wig.PriorityContext != workItems.Peek().Item1)
//            {
//                _logger.Info($"{System.Reflection.MethodBase.GetCurrentMethod().Name} {workItemGroup}, {task}, Changing priority from {wig.PriorityContext} to {workItems.Peek().Item1}");
//            }
#endif
            // wig.PriorityContext = workItems.Peek().Item1;
        }

        public void OnAddWIGToRunQueue(Task task, WorkItemGroup wig)
        {
            
            var priority = workItems.Count > 0 ? workItems.Peek().Item1 : SchedulerConstants.DEFAULT_PRIORITY;
#if DEBUG
//            _logger.Info(
//                $"workitem queue: {GetWorkItemQueueStatus()}");
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
            wig.PriorityContext = priority;
            //            if (wig.PriorityContext < priority)
            //            {
            //                wig.PriorityContext = priority;
            //#if DEBUG
            //                _logger.Info($"{System.Reflection.MethodBase.GetCurrentMethod().Name} {task}: {wig.PriorityContext}, {priority}");
            //#endif
            //            }
        }

        public void OnClosingWIG()
        {
            foreach (var task in workItems)
            {
                task.Item2.Ignore();
            }
            workItems.Clear();
        }

        public Task GetNextTaskForExecution()
        {
            if (workItems.Count > 0)
            {
                return workItems.Take().Item2;
            }

            // TODO: reinsert WIG if priority change?
            return null;
        }

        public int CountWIGTasks()
        {
            return workItems.Count;
        }

        public Task GetOldestTask()
        {
            // TODO: Not really the oldest task
            return workItems.Peek().Item2;
        }

        public string GetWorkItemQueueStatus()
        {
            return string.Join(",", workItems.Select( x=>x.Item1 + " - " + x.Item2));
        }

        public void OnReAddWIGToRunQueue() { }

        public string ExplainDependencies()
        {
            return "UpStreamSet: " + string.Join(",", UpstreamGroups) + ". DownStreamPaths: " +
                   string.Join(";", DownStreamPaths.Select(x => string.Join(" - ",x)));
        }
    }

    internal class TaskComparer : IComparer<Tuple<long, Task>>
    {
        public int Compare(Tuple<long, Task> x, Tuple<long, Task> y)
        {
            return y.Item1.CompareTo(x.Item1);
        }
    }
    */
    
        
    internal class PriorityBasedEDFWorkItemManager : IWorkItemManager
    {
        private SortedDictionary<long, Queue<Task>> workItems;
        private readonly LoggerImpl _logger; 
        private readonly WorkItemGroup workItemGroup;
        private bool dequeuedFlag;
        private int statCollectionCounter = SchedulerConstants.MEASUREMENT_PERIOD_WORKITEM_COUNT;
        private long wid;

        internal List<WorkItemGroup> UpstreamGroups { get; set; } // upstream WIGs groups for backtracking
        internal List<Stack<WorkItemGroup>> DownStreamPaths { get; set; } // downstream WIG paths groups for calculation
        public ISchedulingStrategy Strategy { get; set; }
        internal long DataflowSLA { get; set; }
        public bool WindowedGrain { get; set; }
        public long WindowSize { get; set; }

        public PriorityBasedEDFWorkItemManager(ISchedulingStrategy strategy, WorkItemGroup wig)
        {
            Strategy = strategy;
            workItems = new SortedDictionary<long, Queue<Task>>();
            UpstreamGroups = new List<WorkItemGroup>();
            DownStreamPaths = new List<Stack<WorkItemGroup>>();
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
                long maximumDownStreamPathCost = 0L;
                foreach (var stack in DownStreamPaths)
                {
                    var pathCost = 0L;
                    // collect all metrics along the way
#if PQ_DEBUG
                    _logger.Info(
                        $"{System.Reflection.MethodBase.GetCurrentMethod().Name} {workItemGroup} exploring path {string.Join(" , ", stack)}");
#endif
                    foreach (var elem in stack)
                    {
                        // TODO: fix later with window information
                        var statCollection= (Dictionary<ActivationAddress, Dictionary<string, double>>)Strategy.FetchWorkItemMetric(elem);
                        var address = ((SchedulingContext) workItemGroup.SchedulingContext).Activation.Address;
                        var cost = 0L;
                        if (statCollection.ContainsKey(address) && statCollection[address].ContainsKey(task.ToString())) cost = Convert.ToInt64(statCollection[address][task.ToString()]);
                        pathCost += cost;
#if PQ_DEBUG
                        _logger.Info(
                            $"-----> {elem}: {cost} {pathCost}");
#endif
                    }

                    if (pathCost > maximumDownStreamPathCost) maximumDownStreamPathCost = pathCost;
                }

                var ownerStats = (Dictionary<ActivationAddress, Dictionary<string, double>>) Strategy.FetchWorkItemMetric(workItemGroup);
                if (contextObj.SourceActivation!=null && ownerStats.ContainsKey(contextObj.SourceActivation) && ownerStats[contextObj.SourceActivation].ContainsKey(task.ToString()))
                {
                    maximumDownStreamPathCost += Convert.ToInt64(ownerStats[contextObj.SourceActivation][task.ToString()]);
                }
                // ***
                // Setting priority of the task
                // ***
                var priority = timestamp + DataflowSLA - maximumDownStreamPathCost;

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
            _logger.Info($"{System.Reflection.MethodBase.GetCurrentMethod().Name} {workItemGroup}, {task}, {timestamp} : {WindowSize}: {oldWid} -> {wid} : {DataflowSLA} : {MaximumDownStreamPathCost} : {priority}");
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
            var nextDeadline = Strategy.PeekNextDeadline();
            
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
            /*
            if (workItems.Any() && workItems.First().Value.Any())
            {
                return workItems.First().Value.Dequeue();
            }

            // finish current priority, break and take wig off the queue
            if (!workItems.First().Value.Any()) workItems.Remove(workItems.Keys.First());
            return null;
            */
        }

        public void UpdateWIGStatistics()
        {
            if (--statCollectionCounter <= 0)
            {
                statCollectionCounter = SchedulerConstants.MEASUREMENT_PERIOD_WORKITEM_COUNT;
                Strategy.PutWorkItemMetric(workItemGroup, workItemGroup.CollectStats());
                workItemGroup.LogExecTimeCounters();
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

        public string ExplainDependencies()
        {
            return "UpStreamSet: " + string.Join(",", UpstreamGroups) + ". DownStreamPaths: " +
                   string.Join(";", DownStreamPaths.Select(x => string.Join("-",x)));
        }
    }
}
