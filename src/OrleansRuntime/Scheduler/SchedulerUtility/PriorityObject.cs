using System;
using System.Collections.Generic;

namespace Orleans.Runtime.Scheduler.SchedulerUtility
{
    public struct PriorityObject : IComparable
    {
        public long Priority;
        public long WindowID;
        public int Ticks;

        public PriorityObject(long priority, int ticks, long windowId = SchedulerConstants.DEFAULT_WINDOW_ID)
        {
            Priority = priority;
            Ticks = ticks;
            WindowID = windowId;
        }

        public int CompareTo(object obj)
        {
            if (obj == null) return 1;
            var compareTo = (PriorityObject)obj;
            if (Priority != compareTo.Priority) return Priority.CompareTo(compareTo.Priority);
            return Ticks.CompareTo(compareTo.Ticks);
        }

        public override String ToString()
        {
            return Priority + ":" +WindowID + ":" + Ticks;
        }

        public void Update()
        {
            Ticks++;
        }

        public PriorityObject Default()
        {
            Priority = SchedulerConstants.DEFAULT_PRIORITY;
            WindowID = SchedulerConstants.DEFAULT_WINDOW_ID;
            Ticks = Environment.TickCount;
            return this;
        }
    }
}
