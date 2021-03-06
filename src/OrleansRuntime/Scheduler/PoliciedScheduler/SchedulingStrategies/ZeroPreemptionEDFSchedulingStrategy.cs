﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Orleans.Runtime.Scheduler.SchedulerUtility;

namespace Orleans.Runtime.Scheduler.PoliciedScheduler.SchedulingStrategies
{
    internal class ZeroPreemptionEDFSchedulingStrategy : ISchedulingStrategy
    {
       private LoggerImpl _logger; // = LogManager.GetLogger(this.GetType().FullName, LoggerType.Runtime);

        #region Tenancies
        public ConcurrentDictionary<WorkItemGroup, long> TenantCostEstimate { get; set; }

        private Dictionary<ActivationAddress, WorkItemGroup> addressToWIG;
        private ConcurrentDictionary<ulong, long> windowedKeys;
        private int statCollectionCounter = SchedulerConstants.MEASUREMENT_PERIOD_WORKITEM_COUNT;

        #endregion

        public IOrleansTaskScheduler Scheduler { get; set; }
 
        public IComparable GetPriority(IWorkItem workItem)
        {
            if (Scheduler.GetWorkItemGroup(workItem.SchedulingContext) != null) return workItem.PriorityContext;
            return SchedulerConstants.DEFAULT_PRIORITY;
        }

        public void Initialization()
        {
            TenantCostEstimate = new ConcurrentDictionary<WorkItemGroup, long>(2, 31);
            addressToWIG = new Dictionary<ActivationAddress, WorkItemGroup>();
            _logger = LogManager.GetLogger(this.GetType().FullName, LoggerType.Runtime);
            windowedKeys = new ConcurrentDictionary<ulong, long>();
        }

        public void OnWorkItemInsert(IWorkItem workItem, WorkItemGroup wig)
        {    
            // Collect stat from WIGs
            if (TenantCostEstimate.Any() && --statCollectionCounter <= 0) 
            {
                statCollectionCounter = SchedulerConstants.MEASUREMENT_PERIOD_WORKITEM_COUNT;
                // TODO: fix single level counter
                // foreach (var kv in TenantCostEstimate.ToArray()) TenantCostEstimate[kv.Key] = (long)kv.Key.CollectStats();
                _logger.Info($"Printing execution times in ticks: {string.Join("********************", TenantCostEstimate.Select(x => x.Key.ToString() + ':' + x.Value))}");
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
                //if(!windowedKeys.ContainsKey(k)) windowedKeys.Add(k, controllerContext.windowedKey[k]);
                windowedKeys.AddOrUpdate(k, controllerContext.windowedKey[k], (key, value) => value);
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
            var workitemManager = wig.WorkItemManager as ZeroPreemptionEDFWorkItemManager;
            workitemManager.DataflowSLA = (long) controllerContext.Time;
            if (!TenantCostEstimate.ContainsKey(wig)) TenantCostEstimate.TryAdd(wig, SchedulerConstants.DEFAULT_WIG_EXECUTION_COST);
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
            var workItemManager = upstreamWig.WorkItemManager as ZeroPreemptionEDFWorkItemManager;
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
            wig.WorkItemManager = new ZeroPreemptionEDFWorkItemManager(this, wig);
            if (context.ContextType.Equals(SchedulingContextType.Activation) && windowedKeys.Keys.Contains(((SchedulingContext) context).Activation.Grain.Key.N1))
            {
                var wim = wig.WorkItemManager as ZeroPreemptionEDFWorkItemManager;
                wim.WindowedGrain = true;
                wim.WindowSize = windowedKeys[((SchedulingContext) context).Activation.Grain.Key.N1];
            }
            // populate addressToWIG for fast lookup
            if (context.ContextType.Equals(SchedulingContextType.Activation) && !addressToWIG.ContainsKey(((SchedulingContext)context).Activation.Address))
                addressToWIG[((SchedulingContext)context).Activation.Address] = wig;
            return wig;
        }

        public long FetchWorkItemMetric(WorkItemGroup workItem)
        {
            return TenantCostEstimate.ContainsKey(workItem) ? TenantCostEstimate[workItem] : SchedulerConstants.DEFAULT_WIG_EXECUTION_COST;
        }

        public void PutWorkItemMetric(WorkItemGroup workItemGroup, object metric)
        {
            if (TenantCostEstimate.ContainsKey(workItemGroup)) TenantCostEstimate[workItemGroup] = (long) metric;
        }

        public long PeekNextDeadline()
        {
            throw new NotImplementedException();
        }

    }
        
    internal class ZeroPreemptionEDFWorkItemManager : IWorkItemManager
    {
        private SortedDictionary<long, Queue<Task>> workItems;
        private readonly LoggerImpl _logger; //  = LogManager.GetLogger("Scheduler.PoliciedScheduler.SchedulingStrategies", LoggerType.Runtime);
        private readonly WorkItemGroup workItemGroup;
        private int statCollectionCounter = SchedulerConstants.MEASUREMENT_PERIOD_WORKITEM_COUNT;
        internal List<WorkItemGroup> UpstreamGroups { get; set; } // upstream WIGs groups for backtracking
        internal List<Stack<WorkItemGroup>> DownStreamPaths { get; set; } // downstream WIG paths groups for calculation
        public ISchedulingStrategy Strategy { get; set; }
        internal long MaximumDownStreamPathCost { get; set; }
        internal long DataflowSLA { get; set; }
        public bool WindowedGrain { get; set; }
        public long WindowSize { get; set; }

        public ZeroPreemptionEDFWorkItemManager(ISchedulingStrategy strategy, WorkItemGroup wig)
        {
            Strategy = strategy;
            workItems = new SortedDictionary<long, Queue<Task>>();
            UpstreamGroups = new List<WorkItemGroup>();
            DownStreamPaths = new List<Stack<WorkItemGroup>>();
            MaximumDownStreamPathCost = 0L;
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
                        var cost = Strategy.FetchWorkItemMetric(elem);
                        pathCost += cost;
#if PQ_DEBUG
                        _logger.Info(
                            $"-----> {elem}: {cost} {pathCost}");
#endif
                    }
                    pathCost = Strategy.FetchWorkItemMetric(workItemGroup);
                    if (pathCost > MaximumDownStreamPathCost) MaximumDownStreamPathCost = pathCost;
                }
                // ***
                // Setting priority of the task
                // ***
                var priority = timestamp + DataflowSLA - MaximumDownStreamPathCost;
                if (WindowedGrain)
                {
                    priority = (timestamp / WindowSize + 1) * WindowSize + DataflowSLA - MaximumDownStreamPathCost;
                }

                    if (!workItems.ContainsKey(priority))
                {
                    workItems.Add(priority, new Queue<Task>());
                }
#if PQ_DEBUG
            _logger.Info($"{System.Reflection.MethodBase.GetCurrentMethod().Name} {workItemGroup}, {task}, {timestamp} : {DataflowSLA} : {MaximumDownStreamPathCost} : {priority}");
#endif
                workItems[priority].Enqueue(task);

            }
            else
            {
                var priority = workItems.Any()?workItems.Keys.First():wig.PriorityContext.Priority;
                if (!workItems.ContainsKey(priority))
                {
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
            wig.PriorityContext = new PriorityObject(priority, Environment.TickCount);
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

            if (workItems.Any() && workItems.First().Value.Any())
            {
                    return workItems.First().Value.Dequeue();
            }

            // finish current priority, break and take wig off the queue
            if(!workItems.First().Value.Any()) workItems.Remove(workItems.Keys.First());
            return null;
        }

        public void UpdateWIGStatistics()
        {
            if (--statCollectionCounter <= 0)
            {
                statCollectionCounter = SchedulerConstants.MEASUREMENT_PERIOD_WORKITEM_COUNT;
                Strategy.PutWorkItemMetric(workItemGroup, workItemGroup.CollectStats());
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
        }

        public string ExplainDependencies()
        {
            return "UpStreamSet: " + string.Join(",", UpstreamGroups) + ". DownStreamPaths: " +
                   string.Join(";", DownStreamPaths.Select(x => string.Join("-",x)));
        }
    }
}
