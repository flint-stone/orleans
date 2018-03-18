using System;
using System.Threading.Tasks;


namespace Orleans.Runtime.Scheduler
{
    internal class TaskSchedulerUtils
    {
        internal static Task WrapWorkItemAsTask(IWorkItem todo, PriorityContext context, TaskScheduler sched)
        {
            var task = new Task(state => RunWorkItemTask(todo, sched), context);
            return task;
        }

        private static void RunWorkItemTask(IWorkItem todo, TaskScheduler sched)
        {
            try
            {
                RuntimeContext.SetExecutionContext(todo.SchedulingContext, sched);
                todo.Execute();
            }
            finally
            {
                RuntimeContext.ResetExecutionContext();
            }
        }
    }

    public class PriorityContext
    {
        internal double timeRemain;
        internal ISchedulingContext context { get; set; }

        public override String ToString()
        {
            return context.ToString() + " : " + timeRemain;
        }
    }
}
