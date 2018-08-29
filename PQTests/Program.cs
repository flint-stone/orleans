using System;
using Orleans;
using Orleans.Runtime.Configuration;
using Orleans.Runtime.Host;
using TestGrainInterfaces;


namespace PQTests
{
    public class Program
    {
        public static void Main(string[] args)
        {
            // First, configure and start a local silo
            var siloConfig = ClusterConfiguration.LocalhostPrimarySilo();
            var silo = new SiloHost("TestSilo", siloConfig);
            silo.InitializeOrleansSilo();
            silo.StartOrleansSilo();

            Console.WriteLine("Silo started.");

            

            Console.WriteLine("\nPress Enter to terminate...");
            Console.ReadLine();

            // Shut down

            silo.ShutdownOrleansSilo();
        }
    }
}
