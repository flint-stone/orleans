//#define PQ_DEBUG
//#define PQ_DEBUG_DETAILED
using Orleans.Runtime.Scheduler.SchedulerUtility;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;

namespace Orleans.Runtime.Scheduler.PoliciedScheduler.SchedulingStrategies
{
    internal class StatisticsManager
    {

        private LoggerImpl _logger;
        private int statCollectionCounter;
        private readonly WorkItemGroup workItemGroup;
        private readonly ICorePerformanceMetrics metrics;
        private ConcurrentDictionary<GrainId, ExecTimeCounter> _execTimeCounters;
        private ConcurrentDictionary<GrainId, Tuple<double, long>> StatsUpdatesCollection { get; set; }

        internal ConcurrentBag<GrainId> UpstreamOpSet { get; set; } // upstream Ops, populated during initialization
        internal ConcurrentDictionary<GrainId, long> DownstreamOpToCost { get; set; } // downstream Ops, populated while downstream message flows back

        // Cached results
        internal ConcurrentDictionary<GrainId, double> ExecTimeSummaries { get; set; }

        public StatisticsManager(WorkItemGroup wig, ICorePerformanceMetrics perfMetrics)
        {           
            _logger = LogManager.GetLogger(this.GetType().FullName, LoggerType.Runtime);
            statCollectionCounter = SchedulerConstants.MEASUREMENT_PERIOD_WORKITEM_COUNT;
            workItemGroup = wig;
            metrics = perfMetrics;
            StatsUpdatesCollection = new ConcurrentDictionary<GrainId, Tuple<double, long>>();

            UpstreamOpSet = new ConcurrentBag<GrainId>();
            DownstreamOpToCost = new ConcurrentDictionary<GrainId, long>();
            _execTimeCounters = new ConcurrentDictionary<GrainId, ExecTimeCounter>();
            ExecTimeSummaries = new ConcurrentDictionary<GrainId, double>();
        }
        public void GetDownstreamContext(ActivationAddress downstreamActivation, DownstreamContext downstreamContext)
        {
            // TODO: FIX LATER
#if PQ_DEBUG
                    _logger.Info($"{System.Reflection.MethodBase.GetCurrentMethod().Name} {workItemGroup} <- {downstreamActivation} {downstreamContext}");
#endif
            // If coming from upstream or client then ignore
            if (UpstreamOpSet.Contains(downstreamActivation.Grain) || downstreamActivation.Grain.IsClient) return;
            var maxDownstreamCost = downstreamContext.DownStreamCostSum;
            DownstreamOpToCost.AddOrUpdate(downstreamActivation.Grain, maxDownstreamCost, (k, v) => maxDownstreamCost);
        }

        public DownstreamContext CheckForStatsUpdate(GrainId upstream)
        {
            Tuple<double, long> tuple;
#if PQ_DEBUG
            //ReportDetailedStats();
            _logger.Info($"{System.Reflection.MethodBase.GetCurrentMethod().Name} {workItemGroup} Upstream: {upstream} " +
                         $"StatsUpdate collection: {string.Join(",", StatsUpdatesCollection.Select(kv => kv.Key + "-><" + kv.Value.Item1 + "," + kv.Value.Item2 + '>'))}");
#endif

            if ((UpstreamOpSet.Contains(upstream) || !DownstreamOpToCost.Keys.Contains(upstream)) &&
                StatsUpdatesCollection.TryGetValue(upstream, out tuple))
            {
                return new DownstreamContext(
                    tuple.Item2,
                    (long) tuple.Item1,
                    SchedulerConstants.DEFAULT_WIG_EXECUTION_COST,
                    Convert.ToInt64(metrics.InboundAverageWaitingTime),
                    metrics.InboundAverageTripTimeBySource.ContainsKey(upstream.IdentityString)
                        ? Convert.ToInt64(metrics.InboundAverageTripTimeBySource[upstream.IdentityString])
                        : SchedulerConstants.DEFAULT_WIG_EXECUTION_COST);
            }

            
            return null;
        }

        public void UpdateWIGStatistics()
        {
            // Only update statistics here
            if (--statCollectionCounter <= 0)
            {
                statCollectionCounter = SchedulerConstants.MEASUREMENT_PERIOD_WORKITEM_COUNT;
                // workItemGroup.CollectStats();
                // var statsToReport = workItemGroup.WorkItemGroupStats;
                foreach (var address in _execTimeCounters.Keys)
                {
                    // TODO: update max per message cost - per upstream
                    var statsUpdate = _execTimeCounters[address].Average();
#if PQ_DEBUG_DETAILED
                    _logger.Info($"{workItemGroup} : {_execTimeCounters[address].ToLongString()}");
#endif
                    ExecTimeSummaries.AddOrUpdate(address, statsUpdate, (k, v) => statsUpdate);
                    var downstreamCost = DownstreamOpToCost.Values.Any()
                        ? DownstreamOpToCost.Values.Max()
                        : SchedulerConstants.DEFAULT_WIG_EXECUTION_COST;
#if PQ_DEBUG
                    _logger.Info($"{workItemGroup} exec time counters {address} : {statsUpdate}");
                    _logger.Info($"{System.Reflection.MethodBase.GetCurrentMethod().Name} {workItemGroup.Name} -> {address} local cost from target address: {statsUpdate} down stream: {downstreamCost}");
#endif
                    var tup = new Tuple<double, long>(statsUpdate, downstreamCost);
                    StatsUpdatesCollection.AddOrUpdate(address, tup, (k, v) => tup);
                }
#if PQ_DEBUG
                _logger.Info($"{System.Reflection.MethodBase.GetCurrentMethod().Name} {workItemGroup} StatsUpdate collection {string.Join(",", StatsUpdatesCollection.Select(kv => kv.Key + "-><" + kv.Value.Item1 + "," + kv.Value.Item2 + '>'))}");
                ReportDetailedStats();
#endif
            }
        }

        public void Add(PriorityContext context, TimeSpan span)
        {
            if (!_execTimeCounters.ContainsKey(context.SourceActivation.Grain))
            {
                _execTimeCounters.TryAdd(context.SourceActivation.Grain, new ExecTimeCounter(context.SourceActivation.Grain));
            }
            _execTimeCounters[context.SourceActivation.Grain].Increment(context.WindowID, context.RequestId, span.Ticks);
        }

        public void ReportStats()
        {
            _logger.Info($"Average ticks per msg: {ExecTimeSummaries.Values.Average()}" +
                         $"Downstream exec cost average: {DownstreamOpToCost.Values.Average()}" +
                         $"Inbound average: {metrics.InboundAverageWaitingTime}"+
                         $"Remote delivery: {metrics.InboundAverageTripTimeBySource.Values.Average()}" +
                         $"Outbound average: {metrics.OutboundAverageWaitingTime}");
        }

        public void ReportDetailedStats()
        {
             _logger.Info($"============================{workItemGroup}================================== \n" +
                          $"Average ticks per msg: {string.Join(";", ExecTimeSummaries.Select(kv => kv.Key + "->" + kv.Value).ToArray())} \n" +
                         $"Downstream exec cost average: {string.Join(";", DownstreamOpToCost.Select(kv => kv.Key + "->" + kv.Value).ToArray())} \n" +
                         $"Inbound average: {metrics.InboundAverageWaitingTime} \n"+
                         $"Remote delivery: {string.Join(";", metrics.InboundAverageTripTimeBySource.Select(kv => kv.Key + "->" + kv.Value))}" +
                         $"Outbound average: {metrics.OutboundAverageWaitingTime} \n" +
                          $"============================================================== \n");
        }

    }

    internal class ExecTimeCounter
    {
        private long _currentId;
        //private HashSet<long> _currentRequestsSeen;
        
        private long _currentTicksSum;
        //internal Queue<double> Counters;
        internal SortedDictionary<long, double> Counters;
        //private ActivationAddress _source;
        private GrainId _source;
        
        public ExecTimeCounter(GrainId source)
        {
            _source = source;
            Counters = new SortedDictionary<long, double>();
            _currentId = Int64.MinValue;
        }
        
        public void Increment(long id, long requestId, long ticks)
        {
            if (requestId == SchedulerConstants.DEFAULT_REQUEST_ID) return;
            if (!Counters.ContainsKey(requestId))
            {
                Counters.Add(requestId, ticks);
                if (Counters.Count > SchedulerConstants.STATS_COUNTER_QUEUE_SIZE)
                {
                    Counters.Remove(Counters.Keys.First());
                }
            }
            else
            {
                Counters[requestId] += ticks;
            }
        }
        
        public double Average()
        {
            if (!Counters.Any()) return 0;
            return Counters.Values.Average(); 
        }
        
        public override string ToString()
        {
            return $"{Average()}";
        }
        
        public string ToLongString()
        {
            //            return $"Execution counter source {_source}:{_source.Grain.Key.N1}; " +
            //                   $"Counters: {string.Join(",", Counters.Select(kv => kv.Key + "->" + kv.Value))}; ";
            return $"Execution counter source {_source}:{_source.Key.N1}; " +
                   $"Counters: {string.Join(",", Counters.Select(kv => kv.Key + "->" + kv.Value))}; ";
        }

        //        private long _currentId;
        //        private HashSet<long> _currentRequestsSeen;
        //
        //        private long _currentTicksSum;
        //        internal Queue<double> Counters;
        //
        //        private ActivationAddress _source;
        //
        //        public ExecTimeCounter(ActivationAddress source)
        //        {
        //            _source = source;
        //            Counters = new Queue<double>(SchedulerConstants.STATS_COUNTER_QUEUE_SIZE);
        //            _currentRequestsSeen = new HashSet<long>();
        //            _currentId = Int64.MinValue;
        //        }
        //
        //        public void Increment(long id, long requestId, long ticks)
        //        {
        //            if (id > _currentId)
        //            {
        //                Counters.Enqueue(_currentRequestsSeen.Any()?_currentTicksSum/ _currentRequestsSeen.Count:0);
        //                _currentId = id;
        //                _currentRequestsSeen.Clear();
        //                _currentRequestsSeen.Add(requestId);
        //                _currentTicksSum = ticks;
        //                return;
        //            }
        //
        //            if (requestId > 0) _currentRequestsSeen.Add(requestId);
        //            _currentTicksSum += ticks;
        //        }
        //
        //        public double Average()
        //        {
        //            return Counters.Average();
        //        }
        //
        //        public override string ToString()
        //        {
        //            return $"{Average()}";
        //        }
        //
        //        public string ToLongString()
        //        {
        //            return $"Execution counter source {_source}:{_source.Grain.Key.N1}; " +
        //                   $"Counters: {string.Join(",", Counters)}; " +
        //                   $"Tick Sum: {_currentTicksSum}; " +
        //                   $"Requests Seen: <{string.Join(",",_currentRequestsSeen)}>; " +
        //                   $"Window Id: {_currentId}";
        //        }
    }
}
