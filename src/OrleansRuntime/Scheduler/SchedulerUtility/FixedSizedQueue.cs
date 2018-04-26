using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Runtime.Scheduler.Utility
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

}
