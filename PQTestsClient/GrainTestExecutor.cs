using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Orleans;
using Orleans.Runtime;
using Orleans.Runtime.Configuration;
using Orleans.Runtime.Scheduler;
using TestGrainInterfaces;

namespace PQTestsClient
{
    public class GrainTestExecutor
    {
        private IClusterClient _client;
        private int _numGrains;
        private List<ITestGrain> _grains;
        private Timer _timer;

        public bool Running;
        public int Id;


        public GrainTestExecutor(int id, int numGrains, string configFile=null)
        {
            Id = id;
            Running = true;
            _numGrains = numGrains;
            // var clientConfig = ClientConfiguration.LocalhostSilo();
            

            if (GrainClient.IsInitialized) return;

            // Setup Orleans
            ClientConfiguration config;
            if (configFile == null)
            {
                config = ClientConfiguration.LocalhostSilo();
            }
            else
            {
                config = ClientConfiguration.LoadFromFile(configFile);
            }

            // Use high-precesion ticks to generate log file name to avoid conflicts
            var hostname = Dns.GetHostName();
            config.TraceFileName = $"Client-{hostname}-Time-{DateTime.Now.Ticks}.log";

            // Network stack settings
            // config.ResponseTimeout = TimeSpan.FromMinutes(5);
            // config.BufferPoolBufferSize = RuntimeConstants.OrleansBufferPoolBufferSize;


            //GrainClient.Initialize(config);
            _client = new ClientBuilder().UseConfiguration(config).Build();
            _client.Connect().Wait();
            Console.WriteLine($"Client {id} connected.");

            _grains = Enumerable.Range(0, numGrains).Select(x => _client.GetGrain<ITestGrain>(Id*_numGrains + x)).ToList();
        }

        public int Ingest(int seed)
        {
            _timer = new Timer(new TimerCallback(ClosingCallback), null, 240000, 0);
            int count = 0;
            while (Running)
            {
                foreach (var grain in _grains)
                {
                    var time = DateTime.Now.Ticks;
                    var windowSize = 100 * 10000;
                    var context = new TimestampContext
                    {
                        ConvertedLogicalTime = time/windowSize,
                        ConvertedPhysicalTime = time
                    };
                    RequestContext.Set("Priority", context);

                    var fibNumber = grain.Fib(seed).Result;
                                  
                    count++;
                    //Console.WriteLine("\n\n{0} {1} {2} {3}\n\n", fibNumber, Id, grain.GetPrimaryKeyLong(), count++);
                }
                //Task.Delay(1);
            }
            Console.WriteLine("\n\n id {0} count {1} \n\n", Id, count);
            _client.Close();
            return count;
        }

        private void ClosingCallback(object timerState)
        {
            Running = false;
            _timer.Dispose();
        }
    }
}
