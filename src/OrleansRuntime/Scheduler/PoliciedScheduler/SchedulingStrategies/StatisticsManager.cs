using Orleans.Runtime.Scheduler.SchedulerUtility;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace Orleans.Runtime.Scheduler.PoliciedScheduler.SchedulingStrategies
{
    internal class StatisticsManager
    {

        internal ConcurrentBag<GrainId> UpstreamOpSet { get; set; } // upstream Ops, populated during initialization
        internal ConcurrentDictionary<ActivationAddress, long> DownstreamOpToCost { get; set; } // downstream Ops, populated while downstream message flows back

        private LoggerImpl _logger;
        private int statCollectionCounter;
        private readonly WorkItemGroup workItemGroup;
        private readonly ICorePerformanceMetrics metrics;
        private ConcurrentDictionary<GrainId, Tuple<Dictionary<string, long>, long>> StatsUpdatesCollection { get; set; }

        public StatisticsManager(WorkItemGroup wig, ICorePerformanceMetrics perfMetrics)
        {
            UpstreamOpSet = new ConcurrentBag<GrainId>();
            DownstreamOpToCost = new ConcurrentDictionary<ActivationAddress, long>();
            StatsUpdatesCollection = new ConcurrentDictionary<GrainId, Tuple<Dictionary<string, long>, long>>();
            statCollectionCounter = SchedulerConstants.MEASUREMENT_PERIOD_WORKITEM_COUNT;

            workItemGroup = wig;
            metrics = perfMetrics;
            _logger = LogManager.GetLogger(this.GetType().FullName, LoggerType.Runtime);
        }
        public void GetDownstreamContext(ActivationAddress downstreamActivation, DownstreamContext downstreamContext)
        {
            // TODO: FIX LATER
//#if PQ_DEBUG
                    _logger.Info($"{System.Reflection.MethodBase.GetCurrentMethod().Name} {workItemGroup} <- {downstreamActivation} {downstreamContext}");
//#endif
            var maxDownstreamCost = downstreamContext.DownStreamCostSum;
            DownstreamOpToCost.AddOrUpdate(downstreamActivation, maxDownstreamCost, (k, v) => maxDownstreamCost);
        }

        public DownstreamContext CheckForStatsUpdate(GrainId upstream)
        {
            Tuple<Dictionary<string, long>, long> tuple;
            if (StatsUpdatesCollection.TryGetValue(upstream, out tuple))
            {
                return new DownstreamContext(
                    tuple.Item2, 

                    tuple.Item1.Values.Max(),

                    SchedulerConstants.DEFAULT_WIG_EXECUTION_COST,

                    Convert.ToInt64(metrics.InboundAverageWaitingTime),

                    metrics.InboundAverageTripTimeBySource.ContainsKey(upstream.IdentityString)?
                    Convert.ToInt64(metrics.InboundAverageTripTimeBySource[upstream.IdentityString]):
                    SchedulerConstants.DEFAULT_WIG_EXECUTION_COST);
            }
            return null;
        }

        public void UpdateWIGStatistics()
        {
            if (--statCollectionCounter <= 0)
            {
                statCollectionCounter = SchedulerConstants.MEASUREMENT_PERIOD_WORKITEM_COUNT;
                workItemGroup.CollectStats();
                var statsToReport = workItemGroup.WorkItemGroupStats;
                foreach (var address in statsToReport.Keys)
                {
                    var statsUpdate = statsToReport[address];
                    var downstreamCost = DownstreamOpToCost.Values.Any()
                        ? DownstreamOpToCost.Values.Max()
                        : SchedulerConstants.DEFAULT_WIG_EXECUTION_COST;
#if PQ_DEBUG
                    _logger.Info($"{System.Reflection.MethodBase.GetCurrentMethod().Name} {workItemGroup.Name} -> {address} {string.Join(",", statsUpdate.Select(kv => kv.Key + "->" + kv.Value))} {downstreamCost}");
#endif
                    var tup = new Tuple<Dictionary<string, long>, long>(statsUpdate, downstreamCost);
                    StatsUpdatesCollection.AddOrUpdate(address.Grain, tup, (k, v) => tup);

                }
                // workItemGroup.LogExecTimeCounters();
            }
        }
    }
}
