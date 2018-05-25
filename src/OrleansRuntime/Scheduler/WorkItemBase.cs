using System;
using Orleans.Runtime.Scheduler.SchedulerUtility;

namespace Orleans.Runtime.Scheduler
{
    internal abstract class WorkItemBase : IWorkItem
    {

        internal protected WorkItemBase()
        {
        }

        public ISchedulingContext SchedulingContext { get; set; }
        public TimeSpan TimeSinceQueued 
        {
            get { return Utils.Since(TimeQueued); } 
        }

        public abstract string Name { get; }

        public abstract WorkItemType ItemType { get; }

        public DateTime TimeQueued { get; set; }
  
        public PriorityObject PriorityContext { get; set; }
  
        public ActivationAddress SourceActivation { get; set; }

        public abstract void Execute();
        public abstract void Execute(PriorityContext context);

        public bool IsSystemPriority
        {
            get { return SchedulingUtils.IsSystemPriorityContext(this.SchedulingContext); }
        }

        public override string ToString()
        {
            return String.Format("[{0} WorkItem Name={1}, Ctx={2}, Priority={3}]", 
                ItemType, 
                Name ?? "",
                (SchedulingContext == null) ? "null" : SchedulingContext.ToString(),
                PriorityContext
            );
        }
    }
}

