// #define PQ_DEBUG
using System;
using System.Threading.Tasks;
using Orleans.Runtime.Scheduler.SchedulerUtility;

namespace Orleans.Runtime.Scheduler
{
    internal class InvokeWorkItem : WorkItemBase
    {
        public readonly ControllerContext ControllerContext;
        public readonly DownstreamContext DownstreamContext;
        private static readonly Logger logger = LogManager.GetLogger("InvokeWorkItem", LoggerType.Runtime);
        private readonly ActivationData activation;
        private readonly Message message;
        private readonly Dispatcher dispatcher; 

        public InvokeWorkItem(ActivationData activation, Message message, Dispatcher dispatcher)
        {
            if (activation?.GrainInstance == null)
            {
                var str = string.Format("Creating InvokeWorkItem with bad activation: {0}. Message: {1}", activation, message);
                logger.Warn(ErrorCode.SchedulerNullActivation, str);
                throw new ArgumentException(str);
            }

            this.activation = activation;
            this.message = message;
            this.dispatcher = dispatcher;
            SchedulingContext = activation.SchedulingContext;
            // Interpreting Scheduling Context From Application
            if (message?.RequestContextData != null && message.RequestContextData.ContainsKey("Priority"))
            {
                var tsContext = (RuntimePriorityContext)message.RequestContextData["Priority"];
                PriorityContext = new PriorityObject(tsContext.GlobalPriority, default(int), tsContext.Id, tsContext.LocalPriority);
            }
                
            ControllerContext =
                message?.RequestContextData != null && message.RequestContextData.ContainsKey("ControllerContext")
                    ? (ControllerContext) message.RequestContextData["ControllerContext"]
                    : null;

            DownstreamContext = null;
            if (message?.RequestContextData != null && message.RequestContextData.ContainsKey("DownstreamContext"))
            {
                DownstreamContext = (DownstreamContext) message.RequestContextData["DownstreamContext"];
                // message.RequestContextData.Remove("DownstreamContext");
            }
            
            SourceActivation = message.SendingAddress;
#if REPLY_CONTEXT
            message.Start(); //TimeQueued
            logger.Info($"Enqueue message {message.Id} at {Environment.TickCount}");
#endif
            activation.IncrementInFlightCount();
        }

#region Implementation of IWorkItem

        public override WorkItemType ItemType
        {
            get { return WorkItemType.Invoke; }
        }

        public override string Name
        {
            get { return String.Format("InvokeWorkItem:Id={0} {1}", message.Id, message.DebugContext); }
        }

        public override void Execute()
        {
            try
            {
#if REPLY_CONTEXT
                //Queue End
                message.Stop();
                logger.Info($"Dequeue message {message.Id} at {Environment.TickCount}");
#endif
                if (message?.RequestContextData != null && message.RequestContextData.ContainsKey("Priority"))
                {
                    var tsContext = (RuntimePriorityContext) message.RequestContextData["Priority"];
                    message.RequestContextData["Priority"] = tsContext;
//                        new TimestampContext
//                    {
//                        ConvertedLogicalTime = tsContext.ConvertedLogicalTime,
//                        ConvertedPhysicalTime = tsContext.ConvertedPhysicalTime,
//                        RequestId = tsContext.RequestId, 
//                    };
                }
                    

                var grain = activation.GrainInstance;
                var runtimeClient = (ISiloRuntimeClient)grain.GrainReference.RuntimeClient;
#if PQ_DEBUG
                logger.Info($"Invoke: {message}");
#endif
                Task task = runtimeClient.Invoke(grain, this.activation, this.message);
                task.ContinueWith(t =>
                {
                    // Note: This runs for all outcomes of resultPromiseTask - both Success or Fault
                    activation.DecrementInFlightCount();
                    this.dispatcher.OnActivationCompletedRequest(activation, message);
                }).Ignore();
            }
            catch (Exception exc)
            {
                logger.Warn(ErrorCode.InvokeWorkItem_UnhandledExceptionInInvoke, 
                    String.Format("Exception trying to invoke request {0} on activation {1}.", message, activation), exc);

                activation.DecrementInFlightCount();
                this.dispatcher.OnActivationCompletedRequest(activation, message);
            }
        }

        public override void Execute(PriorityContext context)
        {
            try
            {
                //Queue End
#if REPLY_CONTEXT
                message.Stop();
                logger.Info($"Dequeue message {message.Id} at {Environment.TickCount}");
#endif
                if (message?.RequestContextData != null && message.RequestContextData.ContainsKey("Priority"))
                {
                    var tsContext = (RuntimePriorityContext)message.RequestContextData["Priority"];
                    message.RequestContextData["Priority"] = tsContext;
//                        new TimestampContext
//                    {
//                        ConvertedLogicalTime = tsContext.ConvertedLogicalTime,
//                        ConvertedPhysicalTime = tsContext.ConvertedPhysicalTime,
//                        RequestId = tsContext.RequestId,
//                    };
                }

                var grain = activation.GrainInstance;
                var runtimeClient = (ISiloRuntimeClient)grain.GrainReference.RuntimeClient;
#if PQ_DEBUG
                logger.Info($"Invoke: {message}");
#endif
                Task task = runtimeClient.Invoke(grain, this.activation, this.message);
                task.ContinueWith(delegate
                {
#if PQ_DEBUG
                    logger.Info($"InvokeWithContinuation: {message}");
#endif
                    // Note: This runs for all outcomes of resultPromiseTask - both Success or Fault
                    activation.DecrementInFlightCount();
                    this.dispatcher.OnActivationCompletedRequest(activation, message);
#if PQ_DEBUG
                    logger.Info($"Complete Request: {message} on activation {activation}");
#endif
                }, context).Ignore();
            }
            catch (Exception exc)
            {
                logger.Warn(ErrorCode.InvokeWorkItem_UnhandledExceptionInInvoke,
                    String.Format("Exception trying to invoke request {0} on activation {1}.", message, activation), exc);

                activation.DecrementInFlightCount();
                this.dispatcher.OnActivationCompletedRequest(activation, message);
            }
        }

#endregion

        public override string ToString()
        {
            return String.Format("{0} for activation={1} Message={2}", base.ToString(), activation, message);
        }
    }

}
