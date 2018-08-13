using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using C5;

namespace Orleans.Runtime.Scheduler.SchedulerUtility
{
    internal class ConcurrentPriorityWorkQueue : IProducerConsumerCollection<CPQItem> 
    {
        private readonly ReaderWriterLockSlim _lock;
        private IntervalHeap<CPQItem> _priorityQueue;

        public ConcurrentPriorityWorkQueue()
        {
            _priorityQueue = new IntervalHeap<CPQItem>();
            _lock = new ReaderWriterLockSlim();
        }

        public ConcurrentPriorityWorkQueue(IComparer<CPQItem> comparer)
        {
            _priorityQueue = new IntervalHeap<CPQItem>(comparer);
            _lock = new ReaderWriterLockSlim();
        }

        public IEnumerator<CPQItem> GetEnumerator()
        {
            _lock.EnterReadLock();
            try
            {
                return _priorityQueue.ToList().GetEnumerator();
            }
            finally
            {
                _lock.ExitReadLock();
            }           
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public void CopyTo(Array array, int index)
        {
            _lock.EnterReadLock();
            try
            {
                _priorityQueue.CopyTo((CPQItem[])array, index);
            }
            finally
            {
                _lock.ExitReadLock();
            }
        }

        int ICollection.Count
        {
            get
            {
                _lock.EnterReadLock();
                try
                {
                    return _priorityQueue.Count;
                }
                finally
                {
                    _lock.ExitReadLock();
                }

            }
        }  

        public object SyncRoot => throw new NotSupportedException("");
        public bool IsSynchronized => false;

        public void CopyTo(CPQItem[] array, int index)
        {
            _lock.EnterReadLock();
            try
            {
                _priorityQueue.CopyTo(array, index);
            }
            finally
            {
                _lock.ExitReadLock();
            }
        }

        public bool TryAdd(CPQItem item)
        {
            _lock.EnterWriteLock();
            try
            {
                // Add or update
                if (item.InQueue)
                {
                    //update
                    //IPriorityQueueHandle<T> handle = item.Handle;
                    //var str = string.Format("<before ReAdd {0}: {1}>", item.Name, item.Handle == null ? "null" : item.Handle.ToString());
                    //Console.WriteLine(str);
                    _priorityQueue.Replace(item.Handle, item);
                    //ref item.Handle = ref handle;
                    //str = string.Format("<ReAdd {0}: {1}>", item.Name, item.Handle == null ? "null" : item.Handle.ToString());
                    //Console.WriteLine(str);
                    return true;
                }
                else
                {
                    //add
                    item.InQueue = true;
                    var str = string.Format("<Before Add {0}: {1}>", item.Name, item.Handle == null ? "null" : item.Handle.ToString());
                    Console.WriteLine(str);
                    _priorityQueue.Add(ref item.Handle, item);
                    str = string.Format("<Add {0}: {1}>", item.Name, item.Handle == null ? "null" : item.Handle.ToString());
                    Console.WriteLine(str);
                    return true;
                }
            }
            finally
            {
                _lock.ExitWriteLock();
            }
        }

        public bool TryTake(out CPQItem item)
        {
            _lock.EnterWriteLock();
            try
            {
                //item = _priorityQueue.DeleteMax();
                if (_priorityQueue.IsEmpty)
                {
                    item = null;
                    return false;
                }
                item = _priorityQueue.FindMin();
                _priorityQueue.Delete(item.Handle);
                var str = string.Format("<Delete {0}: {1}>", item.Name, item.Handle == null ? "null" : item.Handle.ToString());
                Console.WriteLine(str);
                // item.Handle = null;
                item.InQueue = false;
                return true;
            }
            finally
            {
                _lock.ExitWriteLock();
            }
        }

        public CPQItem[] ToArray()
        {
            _lock.EnterReadLock();
            try
            {
                return _priorityQueue.ToArray();
            }
            finally
            {
                _lock.ExitReadLock();
            }
        }
        
        public CPQItem Peek()
        {
            _lock.EnterReadLock();
            try
            {
                if (_priorityQueue.IsEmpty) return default(CPQItem);
                return _priorityQueue.FindMin();
            }
            finally
            {
                _lock.ExitReadLock();
            }
        }

    }

    internal abstract class CPQItem : IWorkItem, IComparable 
    {
        public IPriorityQueueHandle<CPQItem> Handle;
        public bool InQueue;

        //public IWorkItem WorkItem;

//        public CPQItem(IWorkItem workItem)
//        {
//            WorkItem = workItem;
//            Handle = null;
//            InQueue = false;
//        }

        public int CompareTo(object obj)
        {
            if (obj == null) return 1;
            var other = obj as CPQItem;
            if (PriorityContext == null && other.PriorityContext == null) return 0;
            return PriorityContext.CompareTo(other.PriorityContext);
        }

        public virtual PriorityObject PriorityContext { get; set; }
        public ActivationAddress SourceActivation { get; set; }
        public abstract string Name { get; }
        public abstract WorkItemType ItemType { get; }
        public ISchedulingContext SchedulingContext { get; set; }
        public TimeSpan TimeSinceQueued { get; }
        public DateTime TimeQueued { get; set; }
        public bool IsSystemPriority { get; }
        public abstract void Execute();

        public abstract void Execute(PriorityContext context);
    }

    internal class CPQItemComparer : IComparer<CPQItem>
    {
        public int Compare(CPQItem x, CPQItem y)
        {
            if (x.PriorityContext == null && y.PriorityContext == null) return 0;
            return x.PriorityContext.CompareTo(y.PriorityContext);
        }
    }
}
