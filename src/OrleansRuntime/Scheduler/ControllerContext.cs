using System;
using System.Collections.Generic;

namespace Orleans.Runtime.Scheduler
{
    [Serializable]
    public class ControllerContext
    {
        public short AppId { get; set; }
        public long Time { get; set; }
        public ulong ControllerKey { get; set; }
        public Dictionary<ulong, long> windowedKey { get; set; }

        public ControllerContext(short appId, long time, ulong controllerKey)
        {
            AppId = appId;
            Time = time;
            ControllerKey = controllerKey;
            windowedKey = new Dictionary<ulong, long>();
            windowedKey.Add(281513631416320, 100000000);
            windowedKey.Add(562988608126976, 100000000);
            windowedKey.Add(844463584837632, 100000000);
            windowedKey.Add(1125938561548288, 100000000);
        }
    }
}
