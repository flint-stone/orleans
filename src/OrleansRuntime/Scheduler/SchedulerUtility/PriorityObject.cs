using System;
using System.Collections.Generic;

namespace Orleans.Runtime.Scheduler.SchedulerUtility
{
    public struct PriorityObject : IComparable
    {
        public long Priority;
        public long WindowID;
        public long RequestId;
        public int Ticks;


        public PriorityObject(long priority, int ticks, long requestId = default(long), long windowId = SchedulerConstants.DEFAULT_WINDOW_ID)
        {
            Priority = priority;
            Ticks = ticks;
            RequestId = requestId;
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
    }
}
