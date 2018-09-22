using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DataStructures;
using Orleans.Runtime.Scheduler.SchedulerUtility;

namespace CPQTests
{
    public class Program
    {
        //
        public static void Main(string[] args)
        {
            //ConcurrentPriorityQueue<CPQItem> cpq = new ConcurrentPriorityQueue<CPQItem>(15, new CPQItemComparer());
            ConcurrentPriorityWorkQueue cpq = new ConcurrentPriorityWorkQueue();
            //ConcurrentPriorityWorkQueueAlternative cpq = new ConcurrentPriorityWorkQueueAlternative();
            /*
            List<int> sourceIds = Enumerable.Range(0, numClients).ToList();
            List<GrainTestExecutor> sources = sourceIds.Select(x => new GrainTestExecutor(x, numGrainsPerClient, config))
                .ToList();
            var resultCollection = new ConcurrentBag<int>();
            Parallel.ForEach(sources, source =>
            {
                resultCollection.Add(source.Ingest(numIters));
            });
            */
            int numExecutors = 10;
            List<int> execIds = Enumerable.Range(0, numExecutors).ToList();
            List<PQExecutors<ConcurrentPriorityWorkQueue>> executors = execIds.Select(x => new PQExecutors<ConcurrentPriorityWorkQueue>(x, cpq)).ToList();
            Parallel.ForEach(executors, executor => executor.Run());
            //AssertEqual(cpq);
            Queue<CPQItem> items = new Queue<CPQItem>();
            CPQItem head = null;
            while (cpq.TryTake(out head))
            {
                items.Enqueue(head);
            }
            Console.WriteLine($"Queue: {string.Join("\n --", items)}");
        }

        private static void AssertEqual(IProducerConsumerCollection<CPQItem> cpq)
        {
            
        }
    }

    public class PQExecutors<T> where T: IProducerConsumerCollection<CPQItem>
    {
        private T _queue;
        private HashSet<CPQItem> items;
        private Random seed;
        private int numItems;
        public int Id;
        public PQExecutors(int id, T queue)
        {
            Id = id;
            _queue = queue;
            items = new HashSet<CPQItem>();
            numItems = 10;
            for (int i = 0; i < numItems/2; i++)
            {
                CPQItem item = new CPQItem();
                item.PriorityContext = new PriorityObject
                {
                    Priority =  i,
                    Ticks = Environment.TickCount
                };
                item.Name = Id + ":"+ i.ToString();
                items.Add(item);
            }
            for (int i = numItems / 2; i < numItems; i++)
            {
                CPQItem item = new CPQItem();
                item.PriorityContext = new PriorityObject
                {
                    Priority = i,
                    Ticks = Environment.TickCount
                };
                item.Name = Id + ":" + i.ToString();
                _queue.TryAdd(item);
            }
            seed = new Random();
        }
        public void Run()
        {
            int count = 100;
            while (count-- > 0)
            {
                foreach (var item in items.ToArray())
                {
                    //Dequeue the head
                    CPQItem head = null;
                    if (_queue.TryTake(out head))
                    {
                        items.Add(head);
                        items.Remove(item);
                        item.PriorityContext = new PriorityObject
                        {
                            Priority = seed.Next(0, 10)+100-count,
                            Ticks = Environment.TickCount
                        };
                        Console.WriteLine($"{Id} {item.PriorityContext}");
                        _queue.TryAdd(item);
                    }
                }
            }
        }
    }
}
