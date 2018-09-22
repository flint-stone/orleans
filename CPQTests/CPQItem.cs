using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using C5;
using Orleans.Runtime.Scheduler.SchedulerUtility;

namespace CPQTests
{
    public class CPQItem : IComparable
    {

        public int Value;
        public IPriorityQueueHandle<CPQItem> Handle;
        public bool InQueue;

        public int CompareTo(object obj)
        {
            if (obj == null) return 1;
            var other = obj as CPQItem;
            //if (PriorityContext == null && other.PriorityContext == null) return 0;
            if (Name == other.Name) return 0; // TODO: hack
            return PriorityContext.CompareTo(other.PriorityContext);
        }

        public virtual PriorityObject PriorityContext { get; set; }
        public PriorityObject InQueuePriorityContext { get; set; }
        public string Name { get; set; }

        public override string ToString()
        {
            return Name + " : " + PriorityContext;
        }

       
    }

    internal class CPQItemComparer : IComparer<CPQItem>
    {
        public int Compare(CPQItem x, CPQItem y)
        {
            // if (x.PriorityContext == null && y.PriorityContext == null) return 0;
            if (x.Name == y.Name) return 0;
            return y.PriorityContext.CompareTo(x.PriorityContext);
        }
    }
}
