using System;
using System.Collections.Generic;

namespace Orleans.Runtime.Scheduler.SchedulerUtility
{
    public class PriorityObject : IComparable
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
            var compareTo = obj as PriorityObject;
            if (Priority != compareTo.Priority) return Priority.CompareTo(compareTo.Priority);
            return Ticks.CompareTo(compareTo.Ticks);
        }

        public override String ToString()
        {
            return Priority + ":" +WindowID + ":" + Ticks;
        }
    }
}
