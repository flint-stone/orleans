﻿using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans.Runtime.Scheduler.SchedulerUtility;

namespace CPQTests
{
    public class ConcurrentPriorityWorkQueueAlternative : IProducerConsumerCollection<CPQItem>
    {
        private Object _lockObj;
        private SortedDictionary<PriorityObject, CPQItem> _priorityQueue;

        public ConcurrentPriorityWorkQueueAlternative()
        {
            _lockObj = new Object();
            _priorityQueue = new SortedDictionary<PriorityObject, CPQItem>();
        }

        public IEnumerator<CPQItem> GetEnumerator()
        {
            lock (_lockObj)
            {
                return (IEnumerator<CPQItem>)_priorityQueue.Values.ToList().GetEnumerator();
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
            lock (_lockObj)
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
                            Priority = item.PriorityContext.Priority,
                            WindowID = item.PriorityContext.WindowID,
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
                item.InQueue = false;
                // item.InQueuePriorityContext = null;
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
