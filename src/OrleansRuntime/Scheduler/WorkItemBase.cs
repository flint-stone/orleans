using System;
using Orleans.Runtime.Scheduler.SchedulerUtility;

namespace Orleans.Runtime.Scheduler
{
    internal abstract class WorkItemBase : CPQItem//IWorkItem
    {

        internal protected WorkItemBase()
        {
        }

        public ISchedulingContext SchedulingContext { get; set; }
        public override TimeSpan TimeSinceQueued 
        {
            get { return Utils.Since(TimeQueued); } 
        }

        public abstract override string Name { get; }

        public abstract override WorkItemType ItemType { get; }

        public DateTime TimeQueued { get; set; }
  
        public override PriorityObject PriorityContext { get; set; }= new PriorityObject(SchedulerConstants.DEFAULT_PRIORITY, Environment.TickCount);
  
        public override ActivationAddress SourceActivation { get; set; }

        public abstract override void Execute();
        public abstract override void Execute(PriorityContext context);

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

