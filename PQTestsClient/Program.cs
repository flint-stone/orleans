using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans;
using Orleans.Runtime.Configuration;
using TestGrainInterfaces;

namespace PQTestsClient
{
    class Program
    {
        static void Main(string[] args)
        {
            // Then configure and connect a client.

            //
            // This is the place for your test code.
            //
            //  Parallel.ForEach(srcs, source => source.Ingest().Wait()); 

            //            int numClients = 16;
            //            int numGrainsPerClient = 30;
            //            int numIters = 5;
            int numClients = Int32.Parse(args[0]);
            int numGrainsPerClient = Int32.Parse(args[1]);
            int numIters = Int32.Parse(args[2]);
            string config = null;
            if(args.Length>3)  config = args[3];
            List<int> sourceIds = Enumerable.Range(0, numClients).ToList();
            List<GrainTestExecutor> sources = sourceIds.Select(x => new GrainTestExecutor(x, numGrainsPerClient, config))
                .ToList();
            var resultCollection = new ConcurrentBag<int>();
            Parallel.ForEach(sources, source =>
            {
                resultCollection.Add(source.Ingest(numIters));
            });
            Console.WriteLine($"Total number of requests {resultCollection.Sum()}");
        }
    }
}
