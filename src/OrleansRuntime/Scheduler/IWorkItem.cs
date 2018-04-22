using System;


namespace Orleans.Runtime.Scheduler
{
    internal interface IWorkItem
    {
        // TODO: Change priority to general context
        double PriorityContext { get; set; }
        ActivationAddress SourceActivation { get; set; }
        string Name { get; }
        WorkItemType ItemType { get; }
        ISchedulingContext SchedulingContext { get; set; }
        TimeSpan TimeSinceQueued { get; }
        DateTime TimeQueued { get; set;  }
        bool IsSystemPriority { get; }
        void Execute();
    }
}
