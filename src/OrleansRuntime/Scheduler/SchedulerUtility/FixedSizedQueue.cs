using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace Orleans.Runtime.Scheduler.SchedulerUtility
{
    public class FixedSizedQueue<T> : Queue<T>
    {
        public int Size { get; set; }

        public FixedSizedQueue(int s)
        {
            Size = s;
        }

        public new void Enqueue(T item)
        {
            base.Enqueue(item);
            if (Count > Size)
            {
                Dequeue();
            }
        }

        public String ToString()
        {
            return string.Join(",", ToArray());
        }
    }

    public class FixedSizedConcurrentQueue<T> : ConcurrentQueue<T>
    {
        public int Size { get; set; }

        public FixedSizedConcurrentQueue(int s)
        {
            Size = s;
        }

        public new void Enqueue(T item)
        {
            base.Enqueue(item);
            if (Count > Size)
            {
                T removed;
                base.TryDequeue(out removed);
            }
        }

        public String ToString()
        {
            return string.Join(",", ToArray());
        }
    }

}
