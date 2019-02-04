/*//#define TIMED_EXECUTION
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;

namespace Orleans.Runtime.Scheduler.SchedulerUtility
{
    internal class ConcurrentPriorityWorkQueueTwoLevel : IProducerConsumerCollection<CPQItem>
    {
        private Object _lockObj;
        private SortedDictionary<PriorityObject, List<CPQItem>> _priorityQueue;
        private Stopwatch _stopwatch;
        private int _enqueueCount;
        private int _dequeueCount;

        public ConcurrentPriorityWorkQueueTwoLevel()
        {
            _lockObj = new Object();
            _priorityQueue = new SortedDictionary<PriorityObject, List<CPQItem>>();
            _stopwatch = new Stopwatch();
        }

        public IEnumerator<CPQItem> GetEnumerator()
        {
            lock (_lockObj)
            {
                return _priorityQueue.Values.SelectMany(list => list).Distinct().ToList().GetEnumerator();
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
            lock (_lockObj)
            {
                return AddItem(item);
            }
#endif
        }

//        if (item.InQueue)
//        {
//            //_priorityQueue.Remove(item.InQueuePriorityContext);
//            _priorityQueue[item.InQueuePriorityContext].Remove(item.InQueuePriorityContext.Ticks);
//            if (_priorityQueue[item.InQueuePriorityContext].Count == 0)
//                _priorityQueue.Remove(item.InQueuePriorityContext);
//        }
//        if (_priorityQueue.ContainsKey(item.PriorityContext))
//        {
//            item.InQueue = true;
//            item.PriorityContext = item.InQueuePriorityContext = new PriorityObject
//            {
//                Priority = item.PriorityContext.Priority,
//                WindowID = item.PriorityContext.WindowID,
//                Ticks = _priorityQueue[item.PriorityContext].Last().Key + 1
//            };
//
//            _priorityQueue[item.PriorityContext].Add(item.InQueuePriorityContext.Ticks, item);
//            return true;
//        }
//        else
//    {
//    item.InQueue = true;
//    item.InQueuePriorityContext = item.PriorityContext = new PriorityObject
//    {
//        Priority = item.PriorityContext.Priority,
//        WindowID = item.PriorityContext.WindowID,
//        Ticks = 0
//    };
//    var dictionary_entry = new SortedDictionary<int, CPQItem>();
//    dictionary_entry.Add(item.InQueuePriorityContext.Ticks, item);
//    _priorityQueue.Add(item.InQueuePriorityContext, dictionary_entry);
//    return true;
//    }

        public bool AddItem(CPQItem item)
        {
            if (item.InQueue)
            {
                //_priorityQueue.Remove(item.InQueuePriorityContext);
                _priorityQueue[item.InQueuePriorityContext].Remove(item);//Remove(item.InQueuePriorityContext.Ticks);
                if (_priorityQueue[item.InQueuePriorityContext].Count == 0)
                    _priorityQueue.Remove(item.InQueuePriorityContext);
            }
            if (_priorityQueue.ContainsKey(item.PriorityContext))
            {
                item.InQueue = true;
                item.InQueuePriorityContext = item.PriorityContext;
                _priorityQueue[item.PriorityContext].Add(item);
                return true;
            }
            else
            {
                item.InQueue = true;
                item.InQueuePriorityContext = item.PriorityContext;
                var list_entry = new List<CPQItem>();
                list_entry.Add(item);
                _priorityQueue.Add(item.InQueuePriorityContext, list_entry);
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
            item = _priorityQueue.First().Value[0];
            item.InQueue = false;
            _priorityQueue.First().Value.RemoveAt(0);
            if (_priorityQueue.First().Value.Count == 0) _priorityQueue.Remove(_priorityQueue.First().Key);
            // item.InQueuePriorityContext = null;
            return true;
        }

        public CPQItem[] ToArray()
        {
            lock (_lockObj)
            {
                return _priorityQueue.Values.SelectMany(list => list).Distinct().ToArray();
            }
        }

        public CPQItem Peek()
        {
            lock (_lockObj)
            {
                if (!_priorityQueue.Any()) return null;
                return _priorityQueue.First().Value[0];
            }
        }
    }
}

*/


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
    internal class ConcurrentPriorityWorkQueueTwoLevel : IProducerConsumerCollection<CPQItem>
    {
        private Object _lockObj;
        private SortedDictionary<PriorityObject, SortedDictionary<int, CPQItem>> _priorityQueue;
        private Stopwatch _stopwatch;
        private int _enqueueCount;
        private int _dequeueCount;

        public ConcurrentPriorityWorkQueueTwoLevel()
        {
            _lockObj = new Object();
            _priorityQueue = new SortedDictionary<PriorityObject, SortedDictionary<int, CPQItem>>();
            _stopwatch = new Stopwatch();
        }

        public IEnumerator<CPQItem> GetEnumerator()
        {
            lock (_lockObj)
            {
                return (IEnumerator<CPQItem>) _priorityQueue.Values.Select(kv => kv.Values).SelectMany(list => list).Distinct().ToList().GetEnumerator();
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
            if (item.InQueue)
            {
                //_priorityQueue.Remove(item.InQueuePriorityContext);
                _priorityQueue[item.InQueuePriorityContext].Remove(item.InQueuePriorityContext.Ticks);
                if (_priorityQueue[item.InQueuePriorityContext].Count == 0)
                    _priorityQueue.Remove(item.InQueuePriorityContext);
            }
            if (_priorityQueue.ContainsKey(item.PriorityContext))
            {
                item.InQueue = true;
                item.PriorityContext = item.InQueuePriorityContext = new PriorityObject
                {
                    Priority = item.PriorityContext.Priority,
                    WindowID = item.PriorityContext.WindowID,
                    Ticks = _priorityQueue[item.PriorityContext].Last().Key + 1
                };
                
                _priorityQueue[item.PriorityContext].Add(item.InQueuePriorityContext.Ticks, item);
                return true;
            }
            else
            {
                item.InQueue = true;
                item.InQueuePriorityContext = item.PriorityContext = new PriorityObject
                {
                    Priority = item.PriorityContext.Priority,
                    WindowID = item.PriorityContext.WindowID,
                    Ticks = 0
                };
                var dictionary_entry = new SortedDictionary<int, CPQItem>();
                dictionary_entry.Add(item.InQueuePriorityContext.Ticks, item);
                _priorityQueue.Add(item.InQueuePriorityContext, dictionary_entry);
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
            var kv = _priorityQueue.First().Value.First();

            _priorityQueue.First().Value.Remove(kv.Key);
            item = kv.Value;
            item.InQueue = false;
            if (_priorityQueue.First().Value.Count == 0) _priorityQueue.Remove(_priorityQueue.First().Key);
            // item.InQueuePriorityContext = null;
            return true;
        }

        public CPQItem[] ToArray()
        {
            lock (_lockObj)
            {
                return _priorityQueue.Values.Select(kv => kv.Values).SelectMany(list => list).Distinct().ToArray();
            }
        }

        public CPQItem Peek()
        {
            lock (_lockObj)
            {
                if (!_priorityQueue.Any()) return null;
                return _priorityQueue.First().Value.First().Value;
            }
        }
    }
}
