using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using C5;


namespace CPQTests
{
    public class ConcurrentPriorityWorkQueue : IProducerConsumerCollection<CPQItem>
    {
        
        private readonly Object _lock;
        private IntervalHeap<CPQItem> _priorityQueue;

        public ConcurrentPriorityWorkQueue()
        {
            _priorityQueue = new IntervalHeap<CPQItem>();
            _lock = new Object();
        }

        public ConcurrentPriorityWorkQueue(IComparer<CPQItem> comparer)
        {
            _priorityQueue = new IntervalHeap<CPQItem>(comparer);
            _lock = new Object();
        }

        public IEnumerator<CPQItem> GetEnumerator()
        {

            lock (_lock)
            {
                return _priorityQueue.ToList().GetEnumerator();
            }

        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public void CopyTo(Array array, int index)
        {
            lock (_lock)
            {
                _priorityQueue.CopyTo((CPQItem[])array, index);
            }
        }

        int ICollection.Count => _priorityQueue.Count;


        public object SyncRoot => _lock;
        public bool IsSynchronized => true;

        public void CopyTo(CPQItem[] array, int index)
        {
            lock (_lock)
            {
                _priorityQueue.CopyTo(array, index);
            }
        }

        public bool TryAdd(CPQItem item)
        {
            lock (_lock)
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

                //add
                item.InQueue = true;
                //                    var str = string.Format("<Before Add {0}: {1}>", item.Name, item.Handle == null ? "null" : item.Handle.ToString());
                //                    Console.WriteLine(str);
                _priorityQueue.Add(ref item.Handle, item);
                //                    str = string.Format("<Add {0}: {1}>", item.Name, item.Handle == null ? "null" : item.Handle.ToString());
                //                    Console.WriteLine(str);
                return true;

            }
        }

        public bool TryTake(out CPQItem item)
        {
            lock (_lock)
            {
                //item = _priorityQueue.DeleteMax();
                if (_priorityQueue.IsEmpty)
                {
                    item = null;
                    return false;
                }
                item = _priorityQueue.FindMin();
                _priorityQueue.Delete(item.Handle);
                //                var str = string.Format("<Delete {0}: {1}>", item.Name, item.Handle == null ? "null" : item.Handle.ToString());
                //                Console.WriteLine(str);
                // item.Handle = null;
                item.InQueue = false;
                return true;
            }
        }

        public CPQItem[] ToArray()
        {
            lock (_lock)
            {
                return _priorityQueue.ToArray();
            }
        }

        public CPQItem Peek()
        {
            lock (_lock)
            {
                if (_priorityQueue.IsEmpty) return default(CPQItem);
                return _priorityQueue.FindMin();
            }
        }

    }
    
}
