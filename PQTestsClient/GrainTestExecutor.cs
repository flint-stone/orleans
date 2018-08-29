using System;
using System.Collections.Generic;
using System.Linq;
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


        public GrainTestExecutor(int id, int numGrains)
        {
            Id = id;
            Running = true;
            _numGrains = numGrains;
            var clientConfig = ClientConfiguration.LocalhostSilo();
            _client = new ClientBuilder().UseConfiguration(clientConfig).Build();
            _client.Connect().Wait();
            Console.WriteLine($"Client {id} connected.");

            _grains = Enumerable.Range(0, numGrains).Select(x => _client.GetGrain<ITestGrain>(Id*_numGrains + x)).ToList();
        }

        public int Ingest(int seed)
        {
            _timer = new Timer(new TimerCallback(ClosingCallback), null, 20000, 0);
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
            Console.WriteLine("\n\n{0} {1} \n\n", Id, count);
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
