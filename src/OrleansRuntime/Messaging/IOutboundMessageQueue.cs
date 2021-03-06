using System;
using System.Collections;
using System.Collections.Generic;

namespace Orleans.Runtime.Messaging
{
    /// <summary>
    /// Used for controlling message delverye
    /// </summary>
    internal interface IOutboundMessageQueue : IDisposable
    {
        /// <summary>
        /// Start operation
        /// </summary>
        void Start();

        /// <summary>
        /// Stop operation
        /// </summary>
        void Stop();

        void SendMessage(Message message);

        /// <summary>
        /// Current queue length
        /// </summary>
        int Count { get; }

        IEnumerable<SiloMessageSender> Senders { get; }
    }
}
