﻿using System;
using System.Threading.Tasks;
using Orleans.Runtime;
using Orleans.Runtime.Scheduler;

namespace Orleans.Timers
{
    internal class TimerRegistry : ITimerRegistry
    {
        private readonly IOrleansTaskScheduler scheduler;

        public TimerRegistry(IOrleansTaskScheduler scheduler)
        {
            this.scheduler = scheduler;
        }

        public IDisposable RegisterTimer(Grain grain, Func<object, Task> asyncCallback, object state, TimeSpan dueTime, TimeSpan period)
        {
            var timer = GrainTimer.FromTaskCallback(this.scheduler, asyncCallback, state, dueTime, period, activationData: grain.Data);
            grain.Data.OnTimerCreated(timer);
            timer.Start();
            return timer;
        }
    }
}
