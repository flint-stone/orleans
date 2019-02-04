//#define PQ_DEBUG
//#define PRIORITY_DEBUG
//#define DDL_FETCH
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

            var wim = wig.WorkItemManager as BoundaryBasedEDFWorkItemManager;
            wim.StatManager.GetDownstreamContext(downstreamActivation, downstreamContext);
        }

        public WorkItemGroup CreateWorkItemGroup(IOrleansTaskScheduler ots, ISchedulingContext context)
        {
            var wig = new WorkItemGroup(ots, context);
            wig.WorkItemManager = new BoundaryBasedEDFWorkItemManager(this, wig);
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
                return wim.StatManager.CheckForStatsUpdate(upstream);
            }
            return null;
        }


        public IWorkItem PeekNextDeadline()
        {
            var workItem = ((PriorityBasedTaskScheduler)Scheduler).NextInRunQueue();
            if (workItem != null)
            {
               return workItem;
            }
            return null;
        }

    }


    internal class BoundaryBasedEDFWorkItemManager : IWorkItemManager
    {
        private readonly BoundaryBasedEDFSchedulingStrategy strategy;
        private readonly SortedDictionary<long, Queue<Task>> workItems;
        private readonly SortedDictionary<long, long[]> timestampsToDeadlines;
        private readonly LoggerImpl _logger;
        private readonly WorkItemGroup workItemGroup;
        private bool dequeuedFlag;
        private long wid;
        private long pace = Int64.MinValue;

        internal long DataflowSLA { get; set; }
        public StatisticsManager StatManager { get; set; }
        public bool WindowedGrain { get; set; }
        public long WindowSize { get; set; }

        public BoundaryBasedEDFWorkItemManager(ISchedulingStrategy iSchedulingStrategy, WorkItemGroup wig)
        {
            strategy = (BoundaryBasedEDFSchedulingStrategy)iSchedulingStrategy;          
            workItems = new SortedDictionary<long, Queue<Task>>();
            timestampsToDeadlines = new SortedDictionary<long, long[]>();
            _logger = LogManager.GetLogger(this.GetType().FullName, LoggerType.Runtime);
            workItemGroup = wig;
            dequeuedFlag = false;
            wid = 0L;


            DataflowSLA = SchedulerConstants.DEFAULT_DATAFLOW_SLA;
            StatManager = new StatisticsManager(wig, ((PriorityBasedTaskScheduler)this.strategy.Scheduler).Metrics);
        }

        public void AddToWorkItemQueue(Task task, WorkItemGroup wig)
        {
            var contextObj = task.AsyncState as PriorityContext;
            var windowId = contextObj?.WindowID ?? SchedulerConstants.DEFAULT_WINDOW_ID;
            var priority = contextObj?.Priority ?? SchedulerConstants.DEFAULT_PRIORITY;
            
            // Remap un-tagged task
            if (windowId == SchedulerConstants.DEFAULT_WINDOW_ID)
            {
                var tses = workItems.Keys.Except(new[] { SchedulerConstants.DEFAULT_PRIORITY });
                if (tses.Any())
                {
                    windowId = tses.Min();
                    priority = timestampsToDeadlines[windowId][0];
                }
            }
            
            // Add task to queue
            if (!workItems.ContainsKey(windowId))
            {
                workItems.Add(windowId, new Queue<Task>());
            }
#if PQ_DEBUG
            _logger.Info($"{workItemGroup} Adding task {task} with timestamp {priority}");
#endif

            workItems[windowId].Enqueue(task);

            // Add timestamp mapping to map
            var maximumDownStreamPathCost = SchedulerConstants.DEFAULT_WIG_EXECUTION_COST;
            if (windowId != SchedulerConstants.DEFAULT_WINDOW_ID)
            {

                // ***
                // Setting priority of the task
                // ***
                var deadline = priority;

                if (!timestampsToDeadlines.ContainsKey(windowId))
                {
                    timestampsToDeadlines.Add(windowId, new []{priority, deadline});
                }
                else
                {
                    timestampsToDeadlines[windowId] = new[] { priority, deadline };
                }
            }
            else
            {
                if (!timestampsToDeadlines.ContainsKey(windowId))
                {
                    timestampsToDeadlines.Add(windowId, new[] { SchedulerConstants.DEFAULT_PRIORITY, SchedulerConstants.DEFAULT_PRIORITY});
                }
                else
                {
                    timestampsToDeadlines[windowId] = new[] { SchedulerConstants.DEFAULT_PRIORITY, SchedulerConstants.DEFAULT_PRIORITY };
                }
            }

#if PRIORITY_DEBUG
            _logger.Info($"{workItemGroup} Creating New Timestamp, Task: {task},  " +
                         $"Priority: {priority}, " +
                         $"WindowID: {windowId}, " +
                         $"Window Size: {WindowSize}, " +
                         $"SLA: {DataflowSLA} " +
                         $"mappedPriority: {timestampsToDeadlines[windowId][0]}, {timestampsToDeadlines[windowId][1]}");
#endif

#if PQ_DEBUG
            _logger.Info($"{System.Reflection.MethodBase.GetCurrentMethod().Name} {workItemGroup}"+ 
                Environment.NewLine+
                "WorkItemQueueStatus: "+ wig + 
                Environment.NewLine+
                $"{GetWorkItemQueueStatus()}");
#endif
        }

        public bool OnAddWIGToRunQueue(Task task, WorkItemGroup wig)
        {
            dequeuedFlag = true;
            var priority = PeekNextDeadline();
            priority = priority / SchedulerConstants.PRIORITY_GRANULARITY_TICKS * SchedulerConstants.PRIORITY_GRANULARITY_TICKS;
            var oldPriority = wig.PriorityContext.Priority;
            
#if PQ_DEBUG
            _logger.Info($"OnAddWIGToRunQueue: {wig}:{wig.PriorityContext.Priority}:{wig.PriorityContext.Ticks}");
#endif
            wig.PriorityContext = new PriorityObject(priority, Environment.TickCount);
            if (oldPriority == priority) return false;
            return true;
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
            var nextDeadline = SchedulerConstants.DEFAULT_PRIORITY;
            

            while (workItems.Any() && workItems.First().Value.Count == 0)
            {
#if PQ_DEBUG
                _logger.Info($"{System.Reflection.MethodBase.GetCurrentMethod().Name} {workItemGroup} Removing priority, {workItems.Keys.First()}");
#endif
                var currentTime = workItems.First().Key;
                if (timestampsToDeadlines.First().Key < currentTime - WindowSize)
                {
                    // Start cleaning process
                    foreach (var ts in timestampsToDeadlines.Keys.ToArray())
                    {
                        if (ts < currentTime)
                        {
                            timestampsToDeadlines.Remove(ts);
                        }
                        else
                        {
                            break;
                        }
                    }
                }
               
                workItems.Remove(workItems.Keys.First());
//                var elapsed = workItemGroup.QuantumElapsed;
//                if (elapsed > SchedulerConstants.SCHEDULING_QUANTUM_MINIMUM_MILLIS)
//                {
//                    var nextItem = strategy.PeekNextDeadline();
//                    if (nextItem != null)
//                    {
//#if DDL_FETCH
//                _logger.Info($"{System.Reflection.MethodBase.GetCurrentMethod().Name} " +
//                             $"CurrentRunning: {workItemGroup} " +
//                             $"NextItem: {nextItem} " +
//                             $"Priority: {nextItem.PriorityContext.Priority}");
//#endif
//                        nextDeadline = nextItem.PriorityContext.Priority;
//                    }
//                }              
            }


            if (workItems.Count > 0 && (nextDeadline == SchedulerConstants.DEFAULT_PRIORITY 
                                        || timestampsToDeadlines[workItems.First().Key][1] <= nextDeadline 
                                        || dequeuedFlag))
                //if (workItems.Any())
                {
                    var item = workItems.First().Value.Dequeue();
                    dequeuedFlag = false;
                    return item;
                }
            else
            {
#if DDL_FETCH
                if( timestampsToDeadlines[workItems.First().Key][1] > nextDeadline)
                    _logger.Info($"Giving up CPU from with {workItemGroup} priority {timestampsToDeadlines[workItems.First().Key][1]}  to next ddl {nextDeadline}");
#endif
            }

            return null;
        }

        public void OnCompleteTask(PriorityContext context, TimeSpan taskLength)
        {
            // TODO: expensive -- add sampling
            if (context?.SourceActivation == null) return;
            StatManager.Add(context, taskLength);
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
                    $"Deadline  <{timestampsToDeadlines[x.Key][0]} : {timestampsToDeadlines[x.Key][1]}>" +
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
            priority = priority / SchedulerConstants.PRIORITY_GRANULARITY_TICKS * SchedulerConstants.PRIORITY_GRANULARITY_TICKS;

            wig.PriorityContext = new PriorityObject(priority, Environment.TickCount);
#if PQ_DEBUG
            _logger.Info($"OnAddWIGToRunQueue: {wig}:{wig.PriorityContext.Priority}:{wig.PriorityContext.Ticks}");
#endif
            dequeuedFlag = true;
        }

        public long PeekNextDeadline()
        {
            lock (workItemGroup.lockable)
            {
                var tses = workItems.Keys.Except(new[] { SchedulerConstants.DEFAULT_PRIORITY });
                if (tses.Any()) return timestampsToDeadlines[tses.Min()][1];

                return SchedulerConstants.DEFAULT_PRIORITY;
            }
        }
    }
}
