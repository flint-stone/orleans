using System;
using System.Collections.Generic;
using Orleans.Runtime.Scheduler.SchedulerUtility;


namespace Orleans.Runtime.Scheduler
{
    internal interface IWorkItem 
    {
        // TODO: Change priority to general context
        PriorityObject PriorityContext { get; set; }
        ActivationAddress SourceActivation { get; set; }
        string Name { get; }
        WorkItemType ItemType { get; }
        ISchedulingContext SchedulingContext { get; set; }
        TimeSpan TimeSinceQueued { get; }
        DateTime TimeQueued { get; set;  }
        bool IsSystemPriority { get; }
        void Execute();
        void Execute(PriorityContext context);
    }
}
