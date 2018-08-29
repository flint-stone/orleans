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
            /*
            var clientConfig = ClientConfiguration.LocalhostSilo();
            var client = new ClientBuilder().UseConfiguration(clientConfig).Build();
            client.Connect().Wait();
            */

            

            //
            // This is the place for your test code.
            //
            //  Parallel.ForEach(srcs, source => source.Ingest().Wait()); 

            int numClients = 16;
            int numGrainsPerClient = 30;
            int numIters = 5;
            List<int> sourceIds = Enumerable.Range(0, numClients).ToList();
            List<GrainTestExecutor> sources = sourceIds.Select(x => new GrainTestExecutor(x, numGrainsPerClient))
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
