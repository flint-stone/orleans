using System;
using System.Collections.Generic;
using System.Linq;

namespace Orleans.Runtime.Scheduler
{
    [Serializable]
    public class ControllerContext
    {
        public short AppId { get; }
        public long Time { get; }
        public ulong ControllerKey { get; }
        public Dictionary<ulong, long> windowedKey { get; }

        internal HashSet<ActivationAddress> ActivationSeen { get; set; }

        public ControllerContext(short appId, long time, ulong controllerKey, Dictionary<ulong, long> grainKeysToDurations)
        {
            AppId = appId;
            Time = time;
            ControllerKey = controllerKey;
            windowedKey = grainKeysToDurations
                .Where(kv => kv.Value!=1L)
                .ToDictionary(kv=>kv.Key, kv=>kv.Value);

            ActivationSeen = new HashSet<ActivationAddress>();
        }
    }
}
