using System;

namespace Orleans.Runtime.Scheduler
{
    [Serializable]
    public class ControllerContext
    {
        public short AppId { get; set; }
        public long Time { get; set; }
        public ulong ControllerKey { get; set; }

        public ControllerContext(short appId, long time, ulong controllerKey)
        {
            AppId = appId;
            Time = time;
            ControllerKey = controllerKey;
        }
    }
}
