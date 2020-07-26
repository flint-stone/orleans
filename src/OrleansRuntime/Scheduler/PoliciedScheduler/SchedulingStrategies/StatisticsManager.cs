//#define PQ_DEBUG
//#define PQ_DEBUG_DETAILED
using Orleans.Runtime.Scheduler.SchedulerUtility;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace Orleans.Runtime.Scheduler.PoliciedScheduler.SchedulingStrategies
{
    internal class StatisticsManager
    {

        private LoggerImpl _logger;
        private int _statCollectionCounter;
        private readonly WorkItemGroup _workItemGroup;
        private readonly ICorePerformanceMetrics _metrics;
        private readonly ConcurrentDictionary<ActivationAddress, ExecTimeCounter> _execTimeCounters;

        private ConcurrentDictionary<GrainId, Tuple<double, long>> StatsUpdatesCollection { get; set; }

        internal ConcurrentBag<GrainId> UpstreamOpSet { get; }

        internal ConcurrentDictionary<GrainId, long> DownstreamOpToCost { get; }

        // Cached results
        internal ConcurrentDictionary<GrainId, double> ExecTimeSummaries { get; }

        public StatisticsManager(WorkItemGroup wig, ICorePerformanceMetrics perfMetrics)
        {           
            _logger = LogManager.GetLogger(this.GetType().FullName, LoggerType.Runtime);
            _statCollectionCounter = SchedulerConstants.MEASUREMENT_PERIOD_WORKITEM_COUNT;
            _workItemGroup = wig;
            _metrics = perfMetrics;
            StatsUpdatesCollection = new ConcurrentDictionary<GrainId, Tuple<double, long>>();

            UpstreamOpSet = new ConcurrentBag<GrainId>();
            DownstreamOpToCost = new ConcurrentDictionary<GrainId, long>();
            _execTimeCounters = new ConcurrentDictionary<ActivationAddress, ExecTimeCounter>();
            ExecTimeSummaries = new ConcurrentDictionary<GrainId, double>();
        }
        public void GetDownstreamContext(ActivationAddress downstreamActivation, DownstreamContext downstreamContext)
        {
#if PQ_DEBUG
                    _logger.Info($"{System.Reflection.MethodBase.GetCurrentMethod().Name} {_workItemGroup} <- {downstreamActivation} {downstreamContext}");
#endif
            // If coming from upstream or client then ignore
            if (UpstreamOpSet.Contains(downstreamActivation.Grain) || downstreamActivation.Grain.IsClient) return;
            var maxDownstreamCost = downstreamContext.DownStreamCostSum;
            DownstreamOpToCost.AddOrUpdate(downstreamActivation.Grain, maxDownstreamCost, (k, v) => maxDownstreamCost);
        }

        public DownstreamContext CheckForStatsUpdate(GrainId upstream, Message msg)
        {
            Tuple<double, long> tuple;
#if PQ_DEBUG
            _logger.Info($"{System.Reflection.MethodBase.GetCurrentMethod().Name} {_workItemGroup} Upstream: {upstream} " +
                         $"StatsUpdate collection: {string.Join(",", StatsUpdatesCollection.Select(kv => kv.Key + "-><" + kv.Value.Item1 + "," + kv.Value.Item2 + '>'))}");
#endif

            if ((UpstreamOpSet.Contains(upstream) || !DownstreamOpToCost.Keys.Contains(upstream)) &&
                StatsUpdatesCollection.TryGetValue(upstream, out tuple))
            {

                //                     var d =  new DownstreamContext(
                //                    tuple.Item2,
                //                    (long) tuple.Item1,
                //                    SchedulerConstants.DEFAULT_WIG_EXECUTION_COST,
                //                    Convert.ToInt64(_metrics.InboundAverageWaitingTime),
                //                    _metrics.InboundAverageTripTimeBySource.ContainsKey(upstream.IdentityString)
                //                        ? Convert.ToInt64(_metrics.InboundAverageTripTimeBySource[upstream.IdentityString])
                //                        : SchedulerConstants.DEFAULT_WIG_EXECUTION_COST);
//                var d = new DownstreamContext(
//                    0,
//                    (long)tuple.Item1,
//                    SchedulerConstants.DEFAULT_WIG_EXECUTION_COST,
//                    Convert.ToInt64(_metrics.InboundAverageWaitingTime),
//                    _metrics.InboundAverageTripTimeBySource.ContainsKey(upstream.IdentityString)
//                        ? Convert.ToInt64(_metrics.InboundAverageTripTimeBySource[upstream.IdentityString])
//                        : SchedulerConstants.DEFAULT_WIG_EXECUTION_COST);
                if(msg.RequestContextData.ContainsKey("DownstreamContext"))
                {
                    ((DownstreamContext) msg.RequestContextData["DownstreamContext"]).LocalExecutionCost =
                        (long) tuple.Item1;

                    ((DownstreamContext) msg.RequestContextData["DownstreamContext"]).InboundMessageQueueingDelay =
                        Convert.ToInt64(_metrics.InboundAverageWaitingTime);
                    ((DownstreamContext)msg.RequestContextData["DownstreamContext"]).RemoteDeliveryDelay =
                        _metrics.InboundAverageTripTimeBySource.ContainsKey(upstream.IdentityString)
                            ? Convert.ToInt64(_metrics.InboundAverageTripTimeBySource[upstream.IdentityString])
                            : SchedulerConstants.DEFAULT_WIG_EXECUTION_COST;

                }
#if PQ_DEBUG
                _logger.Info(
                    $"{System.Reflection.MethodBase.GetCurrentMethod().Name} Writing Downstream context {_workItemGroup} Upstream: {upstream}  {d}");
#endif
            }


            return null;
        }

        public void UpdateWIGStatistics()
        {
            // Only update statistics here
            if (--_statCollectionCounter <= 0)
            {
                _statCollectionCounter = SchedulerConstants.MEASUREMENT_PERIOD_WORKITEM_COUNT;
                foreach (var address in _execTimeCounters.Keys)
                {
                    // TODO: update max per message cost - per upstream
                    var statsUpdate = _execTimeCounters[address].Average();
#if PQ_DEBUG_DETAILED
                    _logger.Info($"{_workItemGroup} : {_execTimeCounters[address].ToLongString()}");
#endif
                    ExecTimeSummaries.AddOrUpdate(address.Grain, statsUpdate, (k, v) => statsUpdate);
                    var downstreamCost = DownstreamOpToCost.Values.Any()
                        ? DownstreamOpToCost.Values.Max()
                        : SchedulerConstants.DEFAULT_WIG_EXECUTION_COST;
#if PQ_DEBUG
                    _logger.Info($"{_workItemGroup} exec time counters {address} : {statsUpdate}");
                    _logger.Info($"{System.Reflection.MethodBase.GetCurrentMethod().Name} {_workItemGroup.Name} -> {address} local cost from target address: {statsUpdate} down stream: {downstreamCost}");
#endif
                    var tup = new Tuple<double, long>(statsUpdate, downstreamCost);
                    StatsUpdatesCollection.AddOrUpdate(address.Grain, tup, (k, v) => tup);
                }
#if PQ_DEBUG
                _logger.Info($"{System.Reflection.MethodBase.GetCurrentMethod().Name} {_workItemGroup} StatsUpdate collection {string.Join(",", StatsUpdatesCollection.Select(kv => kv.Key + "-><" + kv.Value.Item1 + "," + kv.Value.Item2 + '>'))}");
                ReportDetailedStats();
#endif
            }
        }

        public void Add(PriorityContext context, TimeSpan span)
        {
            if (!_execTimeCounters.ContainsKey(context.SourceActivation))
            {
                _execTimeCounters.TryAdd(context.SourceActivation, new ExecTimeCounter(context.SourceActivation.Grain));
            }
            _execTimeCounters[context.SourceActivation].Increment(context.LocalPriority, context.RequestId, span.Ticks);
#if PQ_DEBUG
            _logger.Info($"{System.Reflection.MethodBase.GetCurrentMethod().Name} {context.SourceActivation} -> {_workItemGroup.Name} {span.Ticks} Status {_execTimeCounters[context.SourceActivation].ToLongString()}");
#endif
        }

        public void ReportStats()
        {
            _logger.Info($"Average ticks per msg: {ExecTimeSummaries.Values.Average()}" +
                         $"Downstream exec cost average: {DownstreamOpToCost.Values.Average()}" +
                         $"Inbound average: {_metrics.InboundAverageWaitingTime}"+
                         $"Remote delivery: {_metrics.InboundAverageTripTimeBySource.Values.Average()}" +
                         $"Outbound average: {_metrics.OutboundAverageWaitingTime}");
        }

        public void ReportDetailedStats()
        {
             _logger.Info($"============================{_workItemGroup}================================== \n" +
                          $"Average ticks per msg: {string.Join(";", ExecTimeSummaries.Select(kv => kv.Key + "->" + kv.Value).ToArray())} \n" +
                         $"Downstream exec cost average: {string.Join(";", DownstreamOpToCost.Select(kv => kv.Key + "->" + kv.Value).ToArray())} \n" +
                         $"Inbound average: {_metrics.InboundAverageWaitingTime} \n"+
                         $"Remote delivery: {string.Join(";", _metrics.InboundAverageTripTimeBySource.Select(kv => kv.Key + "->" + kv.Value))}" +
                         $"Outbound average: {_metrics.OutboundAverageWaitingTime} \n" +
                          $"============================================================== \n");
        }

    }

    internal class ExecTimeCounter
    {
        private readonly SortedDictionary<long, double> _counters;
        private readonly GrainId _source;
        
        public ExecTimeCounter(GrainId source)
        {
            _source = source;
            _counters = new SortedDictionary<long, double>();
        }
        
        public void Increment(long id, long requestId, long ticks)
        {
            if (requestId == SchedulerConstants.DEFAULT_REQUEST_ID) return;
            if (!_counters.ContainsKey(requestId))
            {
                _counters.Add(requestId, ticks);
                if (_counters.Count > SchedulerConstants.STATS_COUNTER_QUEUE_SIZE)
                {
                    _counters.Remove(_counters.Keys.ToArray().First());
                }
            }
            else
            {
                _counters[requestId] += ticks;
            }
        }
        
        public double Average()
        {
            if (!_counters.Any()) return 0;
            return _counters.Values.ToArray().Average(); 
        }
        
        public override string ToString()
        {
            return $"{Average()}";
        }
        
        public string ToLongString()
        {
            return $"Execution counter source {_source}:{_source.Key.N1}; " +
                   $"Counters: {string.Join(",", _counters.Select(kv => kv.Key + "->" + kv.Value))}; ";
        }
    }
}
