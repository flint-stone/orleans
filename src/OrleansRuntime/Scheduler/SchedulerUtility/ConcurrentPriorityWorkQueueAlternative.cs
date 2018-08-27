using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace Orleans.Runtime.Scheduler.SchedulerUtility
{
    internal class ConcurrentPriorityWorkQueueAlternative : IProducerConsumerCollection<CPQItem>
    {
        private Object _lockObj;
        private SortedDictionary<PriorityObject,CPQItem> _priorityQueue;

        public ConcurrentPriorityWorkQueueAlternative()
        {
            _lockObj = new Object();
            _priorityQueue = new SortedDictionary<PriorityObject, CPQItem>();
        }
        public IEnumerator<CPQItem> GetEnumerator()
        {
            lock (_lockObj)
            {
                return (IEnumerator < CPQItem > )_priorityQueue.Values.ToArray().GetEnumerator();
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
            lock (_lockObj)
            {
                if (_priorityQueue.ContainsKey(item.PriorityContext))
                {
                    while (_priorityQueue.ContainsKey(item.PriorityContext.Update()))
                    {
                        _priorityQueue.Add(item.PriorityContext, item);
                        return true;
                    }
                    throw new Exception("Valid priority object not found!");
                }
                else
                {
                    _priorityQueue.Add(item.PriorityContext, item);
                    return true;
                }
            }
        }

        public bool TryTake(out CPQItem item)
        {
            lock (_lockObj)
            {
                item = null;
                if (!_priorityQueue.Any()) return false;
                var kv = _priorityQueue.First();
                _priorityQueue.Remove(kv.Key);
                item = kv.Value;
                return true;
            }
        }

        public CPQItem[] ToArray()
        {
            lock (_lockObj)
            {
                return _priorityQueue.Values.ToArray();
            }
        }
    }
}
