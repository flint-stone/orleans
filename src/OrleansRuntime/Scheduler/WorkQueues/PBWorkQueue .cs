#define PRIORITIZE_SYSTEM_TASKS

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using DataStructures;
using Orleans.Runtime.Scheduler;
using Orleans.Runtime.Scheduler.SchedulerUtility;

namespace Orleans.Runtime.Scheduler
{


    /// <summary>
    /// Timestamp Based Work Queue
    /// </summary>
    internal class PBWorkQueue : IWorkQueue
    {
        private BlockingCollection<CPQItem> mainQueue;
        private BlockingCollection<CPQItem> systemQueue;
        private BlockingCollection<CPQItem>[] queueArray;
        private readonly QueueTrackingStatistic mainQueueTracking;
        private readonly QueueTrackingStatistic systemQueueTracking;
        private readonly QueueTrackingStatistic tasksQueueTracking;
        private ConcurrentPriorityWorkQueueAlternative cpq;

        public int Length { get { return mainQueue.Count + systemQueue.Count; } }
        public int QueueLength { get { return cpq.Count + systemQueue.Count; } }



        internal PBWorkQueue()
        {
            //cpq = new ConcurrentPriorityQueue<CPQItem>(15, new WorkItemComparer());
            //cpq = new ConcurrentPriorityWorkQueue(new CPQItemComparer());
            cpq = new ConcurrentPriorityWorkQueueAlternative();
            mainQueue = new BlockingCollection<CPQItem>(cpq);
            systemQueue = new BlockingCollection<CPQItem>(new ConcurrentQueue<CPQItem>());
            //systemQueue = new BlockingCollection<IWorkItem>(new ConcurrentPriorityQueue<IWorkItem>(15, new WorkItemComparer()));
            //systemQueue = new BlockingCollection<IWorkItem>(new ConcurrentPriorityQueue<IWorkItem>(new PriorityObjectComparer()));
            //queueArray = new BlockingCollection<IWorkItem>[] { systemQueue, mainQueue };
            queueArray = new BlockingCollection<CPQItem>[] { systemQueue, mainQueue };

            if (!StatisticsCollector.CollectShedulerQueuesStats) return;

            mainQueueTracking = new QueueTrackingStatistic("Scheduler.LevelOne.MainQueue");
            systemQueueTracking = new QueueTrackingStatistic("Scheduler.LevelOne.SystemQueue");
            tasksQueueTracking = new QueueTrackingStatistic("Scheduler.LevelOne.TasksQueue");
            mainQueueTracking.OnStartExecution();
            systemQueueTracking.OnStartExecution();
            tasksQueueTracking.OnStartExecution();          
        }

        public void Add(IWorkItem workItem)
        {
            workItem.TimeQueued = DateTime.UtcNow;

            try
            {
#if PRIORITIZE_SYSTEM_TASKS
                if (workItem.IsSystemPriority)
                {
    #if TRACK_DETAILED_STATS
                    if (StatisticsCollector.CollectShedulerQueuesStats)
                        systemQueueTracking.OnEnQueueRequest(1, systemQueue.Count);
    #endif
                    systemQueue.Add((CPQItem)workItem);
                }
                else
                {
    #if TRACK_DETAILED_STATS
                    if (StatisticsCollector.CollectShedulerQueuesStats)
                        mainQueueTracking.OnEnQueueRequest(1, mainQueue.Count);
    #endif
                    mainQueue.Add((CPQItem)workItem);      
                }
#else
    #if TRACK_DETAILED_STATS
                    if (StatisticsCollector.CollectQueueStats)
                        mainQueueTracking.OnEnQueueRequest(1, mainQueue.Count);
    #endif
                mainQueue.Add(task);
#endif
#if TRACK_DETAILED_STATS
                if (StatisticsCollector.CollectGlobalShedulerStats)
                    SchedulerStatisticsGroup.OnWorkItemEnqueue();
#endif
            }
            catch (InvalidOperationException)
            {
                // Queue has been stopped; ignore the exception
            }
        }

        public IWorkItem Get(CancellationToken ct, TimeSpan timeout)
        {
            try
            {
                //IWorkItem todo;
                CPQItem todo;
#if PRIORITIZE_SYSTEM_TASKS
                // TryTakeFromAny is a static method with no state held from one call to another, so each request is independent, 
                // and it doesn’t attempt to randomize where it next takes from, and does not provide any level of fairness across collections.
                // It has a “fast path” that just iterates over the collections from 0 to N to see if any of the collections already have data, 
                // and if it finds one, it takes from that collection without considering the others, so it will bias towards the earlier collections.  
                // If none of the collections has data, then it will fall through to the “slow path” of waiting on a collection of wait handles, 
                // one for each collection, at which point it’s subject to the fairness provided by the OS with regards to waiting on events. 
                if (BlockingCollection<CPQItem>.TryTakeFromAny(queueArray, out todo, timeout) >= 0)
#else
                if (mainQueue.TryTake(out todo, timeout))
#endif
                {
#if TRACK_DETAILED_STATS
                    if (StatisticsCollector.CollectGlobalShedulerStats)
                    {
                        SchedulerStatisticsGroup.OnWorkItemDequeue();
                    }
#endif
                    return todo;
                }
                return null;
            }
            catch (InvalidOperationException)
            {
                return null;
            }
        }

        public IWorkItem GetSystem(CancellationToken ct, TimeSpan timeout)
        {
            try
            {
                //IWorkItem todo;
                CPQItem todo;
#if PRIORITIZE_SYSTEM_TASKS
                if (systemQueue.TryTake(out todo, timeout))
#else
                if (mainQueue.TryTake(out todo, timeout))
#endif
                {
#if TRACK_DETAILED_STATS
                    if (StatisticsCollector.CollectGlobalShedulerStats)
                    {
                        SchedulerStatisticsGroup.OnWorkItemDequeue();
                    }
#endif
                    return todo;
                }
                
                return null;
            }
            catch (InvalidOperationException)
            {
                return null;
            }
        }


        public void DumpStatus(StringBuilder sb)
        {
            if (systemQueue.Count > 0)
            {
                sb.AppendLine("-- System Queue:");
                foreach (var workItem in systemQueue)
                {
                    sb.AppendFormat("--   {0}", workItem).AppendLine();
                }
            }

            if (mainQueue.Count <= 0) return;

            sb.AppendLine("-- Main Queue:");
            foreach (var workItem in mainQueue)
                sb.AppendFormat("--   {0}", workItem).AppendLine();
        }

        public void RunDown()
        {
            mainQueue.CompleteAdding();
            systemQueue.CompleteAdding();

            if (!StatisticsCollector.CollectShedulerQueuesStats) return;

            mainQueueTracking.OnStopExecution();
            systemQueueTracking.OnStopExecution();
            tasksQueueTracking.OnStopExecution();
        }

        public IWorkItem Peek()
        {
            var ret = cpq.Peek();
            return ret;
        }

        public void Dispose()
        {
            queueArray = null;

            if (mainQueue != null)
            {
                mainQueue.Dispose();
                mainQueue = null;
            }

            if (systemQueue != null)
            {
                systemQueue.Dispose();
                systemQueue = null;
            }

            GC.SuppressFinalize(this);
        }

        
    }
}


// Random for testing
internal class WorkItemComparer : IComparer<IWorkItem>
{
    public int Compare(IWorkItem x, IWorkItem y)
    {
        return y.PriorityContext.CompareTo(x.PriorityContext); 
    }
}