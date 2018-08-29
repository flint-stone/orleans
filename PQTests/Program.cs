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
            ClusterConfiguration config;
            string configStr = null;
            if(args.Length>0) configStr= args[0];
            if (configStr == null)
            {
                config = ClusterConfiguration.LocalhostPrimarySilo();
            }
            else
            {
                config = new ClusterConfiguration();
                config.LoadFromFile(configStr);
            }
            // First, configure and start a local silo
            var siloConfig = config; //ClusterConfiguration.LocalhostPrimarySilo();
            var silo = new SiloHost("TestSilo", siloConfig);
            silo.LoadOrleansConfig();
            silo.InitializeOrleansSilo();
            silo.StartOrleansSilo();
            /*
             options = new Options();
            if (!Parser.Default.ParseArguments(args, options)) return;

            ClusterConfiguration config;
            if (options.Configuration == null)
            {
                config = ClusterConfiguration.LocalhostPrimarySilo();
                config.AddMemoryStorageProvider("GrainStateStore");
                config.AddMemoryStorageProvider(RuntimeConstants.StreamStorageProviderName);
                config.AddSimpleMessageStreamProvider(RuntimeConstants.StreamProviderName, true);
            }
            else
            {
                config = new ClusterConfiguration();
                config.LoadFromFile(options.Configuration);
            }

            // Placement settings
            config.UseStartupType<FlareServiceProviderStartup>();
            
            // Orleans network stack settings
            config.Globals.ResponseTimeout = TimeSpan.FromMinutes(5);
            config.Globals.BufferPoolBufferSize = RuntimeConstants.OrleansBufferPoolBufferSize;

            // Trill settings
            Config.ClearColumnsOnReturn = true; // Disable column reuse to get around a Trill bug.
            Config.DisableMemoryPooling = false;
            Config.DataBatchSize = RuntimeConstants.ClusterColumnarSize;

            config.Defaults.SiloName = options.SiloName ?? Dns.GetHostName();
            if (options.Port != null) config.Defaults.Port = Convert.ToInt32(options.Port);
            if (options.DeploymentId != null) config.Globals.DeploymentId = options.DeploymentId;

            _siloHost = new SiloHost(config.Defaults.SiloName, config);
            _siloHost.LoadOrleansConfig();
             */

            Console.WriteLine("Silo started.");

            

            Console.WriteLine("\nPress Enter to terminate...");
            Console.ReadLine();

            // Shut down

            silo.ShutdownOrleansSilo();
        }
    }
}
