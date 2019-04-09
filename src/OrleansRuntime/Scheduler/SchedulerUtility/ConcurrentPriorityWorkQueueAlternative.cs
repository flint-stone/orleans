//#define TIMED_EXECUTION
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;

namespace Orleans.Runtime.Scheduler.SchedulerUtility
{
    internal class ConcurrentPriorityWorkQueueAlternative : IProducerConsumerCollection<CPQItem>
    {
        private Object _lockObj;
        private SortedDictionary<PriorityObject, CPQItem> _priorityQueue;
        private Stopwatch _stopwatch;
        private int _enqueueCount;
        private int _dequeueCount;

        public ConcurrentPriorityWorkQueueAlternative()
        {
            _lockObj = new Object();
            _priorityQueue = new SortedDictionary<PriorityObject, CPQItem>();
            _stopwatch = new Stopwatch();
        }

        public IEnumerator<CPQItem> GetEnumerator()
        {
            lock (_lockObj)
            {
                return (IEnumerator<CPQItem>) _priorityQueue.Values.ToList().GetEnumerator();
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public void CopyTo(Array array, int index)
        {
            lock (_lockObj)
            {
                _priorityQueue.Values.ToArray().CopyTo(array, index);
            }
        }

        public int Count
        {
            get
            {
                lock (_lockObj)
                {
                    // Console.WriteLine($"{_priorityQueue.Count} {string.Join(",", _priorityQueue.Values.ToList())} ");
                    return _priorityQueue.Count;
                }
            }
        }

        public object SyncRoot => _lockObj;
        public bool IsSynchronized => true;

        public void CopyTo(CPQItem[] array, int index)
        {
            lock (_lockObj)
            {
                _priorityQueue.Values.ToArray().CopyTo(array, index);
            }
        }

        public bool TryAdd(CPQItem item)
        {
#if TIMED_EXECUTION
            lock (_lockObj) {
                if (++_enqueueCount % 1000 == 0)
                {
                    //time execution
                    _stopwatch.Start();
                    bool ret = AddItem(item);
                    _stopwatch.Stop();
                    Console.WriteLine($"Enqueue: {item} Elapsed: {_stopwatch.ElapsedTicks} Count: {Count}");
                    _stopwatch.Reset();
                    return ret;
                }

                return AddItem(item);
            }
#else
            lock (_lockObj) {
                return AddItem(item);
            }
#endif
        }

        public bool AddItem(CPQItem item)
        {
            if (item.InQueue == true)
            {
                _priorityQueue.Remove(item.InQueuePriorityContext);
            }
            if (_priorityQueue.ContainsKey(item.PriorityContext))
            {
                while (_priorityQueue.ContainsKey(item.PriorityContext))
                {
                    item.PriorityContext = new PriorityObject
                    {
                        GlobalPriority = item.PriorityContext.GlobalPriority,
                        LocalPriority = item.PriorityContext.LocalPriority,
                        Ticks = item.PriorityContext.Ticks + 1
                    };
                }
                item.InQueue = true;
                item.InQueuePriorityContext = item.PriorityContext;
                _priorityQueue.Add(item.InQueuePriorityContext, item);
                return true;
            }
            else
            {
                item.InQueue = true;
                item.InQueuePriorityContext = item.PriorityContext;
                _priorityQueue.Add(item.InQueuePriorityContext, item);
                return true;
            }
        }

        public bool TryTake(out CPQItem item)
        {
#if TIMED_EXECUTION
            lock (_lockObj)
            {
                if (++_dequeueCount % 1000 == 0)
                {
                    //time execution 
                    _stopwatch.Start();
                    bool ret = TakeItem(out item);
                    _stopwatch.Stop();
                    Console.WriteLine($"Dequeue: {item} Elapsed: {_stopwatch.ElapsedTicks} Count: {Count}");
                    _stopwatch.Reset();
                    return ret;
                }
                return TakeItem(out item);
            }
#else
            lock (_lockObj)
            {
                return TakeItem(out item);
            }
#endif 
        }

        public bool TakeItem(out CPQItem item)
        {
            item = null;
            if (!_priorityQueue.Any()) return false;
            var kv = _priorityQueue.First();
            _priorityQueue.Remove(kv.Key);
            item = kv.Value;
            item.InQueue = false;
            // item.InQueuePriorityContext = null;
            return true;
        }

        public CPQItem[] ToArray()
        {
            lock (_lockObj)
            {
                return _priorityQueue.Values.ToArray();
            }
        }

        public CPQItem Peek()
        {
            lock (_lockObj)
            {
                if (!_priorityQueue.Any()) return null;
                return _priorityQueue.First().Value;
            }
        }
    }
}
