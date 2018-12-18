using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Runtime.Scheduler.SchedulerUtility
{
    public static class DataflowNamingUtility
    {
        public static long GetStageGrainKey(short dataflowId, short stageIdx, int positionIdx)
        {
            return (long)(ushort)dataflowId << 48 | (long)(ushort)stageIdx << 32 | (uint)positionIdx;
        }

        public static long GetControlGrainKey(short dataflowId)
        {
            return (long)(ushort)dataflowId << 48 | (long)0 << 32;
        }

        public static short GetDataflowId(long grainKey)
        {
            return (short)((grainKey >> 48) & 0xFFFFL);
        }

        public static short GetStageId(long grainKey)
        {
            return (short)(grainKey >> 32 & 0xFFFFL);
        }

        public static int GetPositionId(long grainKey)
        {
            return (int)(grainKey & 0xFFFFFFFFL);
        }

        public static string GetReadableGrainKey(long stageGrainKey)
        {
            return
                $"[dtflw: {GetDataflowId(stageGrainKey)}, stg: {GetStageId(stageGrainKey)}, tsk: {GetPositionId(stageGrainKey)}]";
        }
    }
}
