namespace Orleans.Runtime.Messaging
{
    internal interface IInboundMessageQueue
    {
        QueueTrackingStatistic[] QueueTracking { get; }

        int Count { get; }

        void Stop();

        void PostMessage(Message message);

        Message WaitMessage(Message.Categories type);
    }
}
