using System;
using System.Reflection;

namespace Orleans.Runtime.Scheduler
{
    internal class ClosureWorkItem : WorkItemBase
    {
        private readonly Action continuation;
        private readonly Func<string> nameGetter;
        private static readonly Logger logger = LogManager.GetLogger("ClosureWorkItem", LoggerType.Runtime);

        public override string Name { get { return nameGetter==null ? "" : nameGetter(); } }

        public ClosureWorkItem(Action closure, Message message)
        {
            continuation = closure;
#if TRACK_DETAILED_STATS
            if (StatisticsCollector.CollectGlobalShedulerStats)
            {
                SchedulerStatisticsGroup.OnClosureWorkItemsCreated();
            }
#endif
            this.PriorityContext =
                message?.RequestContextData != null && message.RequestContextData.ContainsKey("Deadline")
                    ? (long) message.RequestContextData["Deadline"] : 0;
        }

        public ClosureWorkItem(Action closure, Func<string> getName, Message message)
        {
            continuation = closure;
            nameGetter = getName;
#if TRACK_DETAILED_STATS
            if (StatisticsCollector.CollectGlobalShedulerStats)
            {
                SchedulerStatisticsGroup.OnClosureWorkItemsCreated();
            }
#endif
            this.PriorityContext =
                message?.RequestContextData != null && message.RequestContextData.ContainsKey("Deadline")
                    ? (long) message.RequestContextData["Deadline"] : 0;
        }

        public ClosureWorkItem(Action closure)
        {
            continuation = closure;
#if TRACK_DETAILED_STATS
            if (StatisticsCollector.CollectGlobalShedulerStats)
            {
                SchedulerStatisticsGroup.OnClosureWorkItemsCreated();
            }
#endif
        }

        public ClosureWorkItem(Action closure, Func<string> getName)
        {
            continuation = closure;
            nameGetter = getName;
#if TRACK_DETAILED_STATS
            if (StatisticsCollector.CollectGlobalShedulerStats)
            {
                SchedulerStatisticsGroup.OnClosureWorkItemsCreated();
            }
#endif
        }
        #region IWorkItem Members

        public override void Execute()
        {
#if TRACK_DETAILED_STATS
            if (StatisticsCollector.CollectGlobalShedulerStats)
            {
                SchedulerStatisticsGroup.OnClosureWorkItemsExecuted();
            }
#endif
#if PQ_DEBUG
            logger.Info("Calling closure work item on grain {0} with closure type {1}",
                (continuation.Target == null) ? "" : continuation.Target.ToString(), 
                ToString());
#endif
            continuation();
        }

        public override WorkItemType ItemType { get { return WorkItemType.Closure; } }

        #endregion

        public override string ToString()
        {
            var detailedName = string.Empty; 
            if (nameGetter == null) // if NameGetter != null, base.ToString() will print its name.
            {
                var continuationMethodInfo = continuation.GetMethodInfo();
                detailedName = string.Format(": {0}->{1}",
                   (continuation.Target == null) ? "" : continuation.Target.ToString(),
                   (continuationMethodInfo == null) ? "" : continuationMethodInfo.ToString());
            }
               

            return string.Format("{0}{1}", base.ToString(), detailedName);
        }
    }
}
