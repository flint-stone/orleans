using System;
using System.Collections.Generic;

namespace Orleans.Runtime.Scheduler
{
    [Serializable]
    public class ControllerContext
    {
        public short AppId { get; set; }
        public long Time { get; set; }

        public ControllerContext(short appId, long time)
        {
            AppId = appId;
            Time = time;
        }
    }
}
