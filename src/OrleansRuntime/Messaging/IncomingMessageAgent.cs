using System;
using System.Runtime.Remoting.Messaging;
using System.Threading;
using Orleans.Runtime.Scheduler;
using Orleans.Runtime.Scheduler.PoliciedScheduler;

namespace Orleans.Runtime.Messaging
{
    internal class IncomingMessageAgent : AsynchAgent
    {
        private readonly IMessageCenter messageCenter;
        private readonly ActivationDirectory directory;
        private readonly IOrleansTaskScheduler scheduler;
        private readonly Dispatcher dispatcher;
        private readonly MessageFactory messageFactory;
        private readonly Message.Categories category;

        internal IncomingMessageAgent(Message.Categories cat, IMessageCenter mc, ActivationDirectory ad, IOrleansTaskScheduler sched, Dispatcher dispatcher, MessageFactory messageFactory) :
            base(cat.ToString())
        {
            category = cat;
            messageCenter = mc;
            directory = ad;
            scheduler = sched;
            this.dispatcher = dispatcher;
            this.messageFactory = messageFactory;
            OnFault = FaultBehavior.RestartOnFault;
        }

        public override void Start()
        {
            base.Start();
            if (Log.IsVerbose3) Log.Verbose3("Started incoming message agent for silo at {0} for {1} messages", messageCenter.MyAddress, category);
        }

        protected override void Run()
        {
            try
            {
#if TRACK_DETAILED_STATS
                if (StatisticsCollector.CollectThreadTimeTrackingStats)
                {
                    threadTracking.OnStartExecution();
                }
#endif
                CancellationToken ct = Cts.Token;
                while (true)
                {
                    // Get an application message
                    var msg = messageCenter.WaitMessage(category, ct);
                    if (msg == null)
                    {
                        if (Log.IsVerbose) Log.Verbose("Dequeued a null message, exiting");
                        // Null return means cancelled
                        break;
                    }

#if TRACK_DETAILED_STATS
                    if (StatisticsCollector.CollectThreadTimeTrackingStats)
                    {
                        threadTracking.OnStartProcessing();
                    }
#endif
                    ReceiveMessage(msg);
#if TRACK_DETAILED_STATS
                    if (StatisticsCollector.CollectThreadTimeTrackingStats)
                    {
                        threadTracking.OnStopProcessing();
                        threadTracking.IncrementNumberOfProcessed();
                    }
#endif
                }
            }
            finally
            {
#if TRACK_DETAILED_STATS
                if (StatisticsCollector.CollectThreadTimeTrackingStats)
                {
                    threadTracking.OnStopExecution();
                }
#endif
            }
        }

        private void ReceiveMessage(Message msg)
        {
            MessagingProcessingStatisticsGroup.OnImaMessageReceived(msg);

            ISchedulingContext context;
            // Find the activation it targets; first check for a system activation, then an app activation
            if (msg.TargetGrain.IsSystemTarget)
            {
                SystemTarget target = directory.FindSystemTarget(msg.TargetActivation);
                if (target == null)
                {
                    MessagingStatisticsGroup.OnRejectedMessage(msg);
                    Message response = this.messageFactory.CreateRejectionResponse(msg, Message.RejectionTypes.Unrecoverable,
                        String.Format("SystemTarget {0} not active on this silo. Msg={1}", msg.TargetGrain, msg));
                    messageCenter.SendMessage(response);
                    Log.Warn(ErrorCode.MessagingMessageFromUnknownActivation, "Received a message {0} for an unknown SystemTarget: {1}", msg, msg.TargetAddress);
                    return;
                }
                context = target.SchedulingContext;
                switch (msg.Direction)
                {
                    case Message.Directions.Request:
                        MessagingProcessingStatisticsGroup.OnImaMessageEnqueued(context);
                        scheduler.QueueWorkItem(new RequestWorkItem(target, msg), context);
                        break;

                    case Message.Directions.Response:
                        MessagingProcessingStatisticsGroup.OnImaMessageEnqueued(context);
                        scheduler.QueueWorkItem(new ResponseWorkItem(target, msg), context);
                        break;

                    default:
                        Log.Error(ErrorCode.Runtime_Error_100097, "Invalid message: " + msg);
                        break;
                }
            }
            else
            {
                // Run this code on the target activation's context, if it already exists
                ActivationData targetActivation = directory.FindTarget(msg.TargetActivation);
                if (targetActivation != null)
                {
                    lock (targetActivation)
                    {
                        var target = targetActivation; // to avoid a warning about nulling targetActivation under a lock on it
                        if (target.State == ActivationState.Valid)
                        {
                            var overloadException = target.CheckOverloaded(Log);
                            if (overloadException != null)
                            {
                                // Send rejection as soon as we can, to avoid creating additional work for runtime
                                dispatcher.RejectMessage(msg, Message.RejectionTypes.Overloaded, overloadException, "Target activation is overloaded " + target);
                                return;
                            }

                            // Run ReceiveMessage in context of target activation
                            context = target.SchedulingContext;
                        }
                        else
                        {
                            // Can't use this activation - will queue for another activation
                            target = null;
                            context = null;
                        }

                        EnqueueReceiveMessage(msg, target, context);
                    }
                }
                else
                {
                    // No usable target activation currently, so run ReceiveMessage in system context
                    EnqueueReceiveMessage(msg, null, null);
                }
            }
        }

        private void EnqueueReceiveMessage(Message msg, ActivationData targetActivation, ISchedulingContext context)
        {
            MessagingProcessingStatisticsGroup.OnImaMessageEnqueued(context);

            if (targetActivation != null) targetActivation.IncrementEnqueuedOnDispatcherCount();

#if PQ_DEBUG
            Log.Info("Queue closure work item with path {0}", msg?.RequestContextData != null && msg.RequestContextData.ContainsKey("Path") ? (string)msg.RequestContextData["Path"] : "null");
#endif
            if (scheduler.GetType() == typeof(PriorityBasedTaskScheduler))
            {
                scheduler.QueueWorkItem(new ClosureWorkItem(() =>
                    {
                        try
                        {
                            dispatcher.ReceiveMessage(msg);
                        }
                        finally
                        {
                            if (targetActivation != null) targetActivation.DecrementEnqueuedOnDispatcherCount();
                        }
                    },
                    () => "Dispatcher.ReceiveMessage", msg), context);
            }
            else
            {
                scheduler.QueueWorkItem(new ClosureWorkItem(() =>
                    {
                        try
                        {
                            dispatcher.ReceiveMessage(msg);
                        }
                        finally
                        {
                            if (targetActivation != null) targetActivation.DecrementEnqueuedOnDispatcherCount();
                        }
                    },
                    () => "Dispatcher.ReceiveMessage"), context);
            }
        }
    }
}
