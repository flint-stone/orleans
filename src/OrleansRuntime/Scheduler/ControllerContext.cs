using System;
using System.Collections.Generic;

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

        public ControllerContext(short appId, long time, ulong controllerKey)
        {
            AppId = appId;
            Time = time;
            ControllerKey = controllerKey;
            windowedKey = new Dictionary<ulong, long>();
            windowedKey.Add(281513631416320, 100000000);
            windowedKey.Add(844463584837632, 100000000);
            windowedKey.Add(562988608126976, 100000000);
            windowedKey.Add(2251838468390912, 100000000);
            windowedKey.Add(1407413538258944, 100000000);
            windowedKey.Add(1125938561548288, 100000000);
            windowedKey.Add(1688888514969600, 100000000);
            windowedKey.Add(1970363491680256, 100000000);
            windowedKey.Add(3377738375233536, 100000000);
            windowedKey.Add(3096263398522880, 100000000);
            windowedKey.Add(2533313445101568, 100000000);
            windowedKey.Add(4222163305365504, 100000000);
            windowedKey.Add(2814788421812224, 100000000);
            windowedKey.Add(3659213351944192, 100000000);
            windowedKey.Add(3940688328654848, 100000000);
            windowedKey.Add(4503638282076160, 100000000);

            ActivationSeen = new HashSet<ActivationAddress>();

        }
    }
}
