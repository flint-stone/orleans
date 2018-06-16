﻿using System;

namespace Orleans.Runtime.Scheduler.PoliciedScheduler.SchedulingStrategies
{
    internal interface ISchedulingStrategy
    {
        IOrleansTaskScheduler Scheduler { get; set; }

        void Initialization();

        void OnWorkItemInsert(IWorkItem workItem, WorkItemGroup wig);

        void OnReceivingControllerInstructions(IWorkItem workItem, ISchedulingContext context);

        WorkItemGroup CreateWorkItemGroup(IOrleansTaskScheduler ots, ISchedulingContext context);
    }
}
