﻿using System;

namespace Orleans.Runtime.Scheduler.PoliciedScheduler.SchedulingStrategies
{
    internal interface ISchedulingStrategy
    {
        IOrleansTaskScheduler Scheduler { get; set; }

        #region IOrleansTaskScheduler

        void CollectStatistics();
        IComparable GetPriority(IWorkItem workItem);
        int GetQuantumNumTasks();
        int GetQuantumMillis();
        void Initialization();
        void OnWorkItemInsert(IWorkItem workItem, WorkItemGroup wig);
        void OnReceivingControllerInstructions(IWorkItem workItem, ISchedulingContext context);
        WorkItemGroup CreateWorkItemGroup(IOrleansTaskScheduler ots, ISchedulingContext context);
        #endregion

    }
}
