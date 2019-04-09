using System;
using System.Collections.Generic;

namespace Orleans.Runtime.Scheduler.SchedulerUtility
{
    public struct PriorityObject : IComparable
    {
        public long GlobalPriority;
        public long LocalPriority;
        public long RequestId;
        public int Ticks;


        public PriorityObject(long globalPriority, int ticks, long requestId = default(long), long localPriority = SchedulerConstants.DEFAULT_WINDOW_ID)
        {
            GlobalPriority = globalPriority;
            Ticks = ticks;
            RequestId = requestId;
            LocalPriority = localPriority;
        }

        public int CompareTo(object obj)
        {
            if (obj == null) return 1;
            var compareTo = (PriorityObject)obj;
            if (GlobalPriority != compareTo.GlobalPriority) return GlobalPriority.CompareTo(compareTo.GlobalPriority);
            return Ticks.CompareTo(compareTo.Ticks);
        }

        public override String ToString()
        {
            return GlobalPriority + ":" +LocalPriority + ":" + Ticks;
        }

        public void Update()
        {
            Ticks++;
        }
    }
}
