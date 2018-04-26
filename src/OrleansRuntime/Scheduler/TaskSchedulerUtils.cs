using System.Threading.Tasks;


namespace Orleans.Runtime.Scheduler
{
    internal class TaskSchedulerUtils
    {
        internal static Task WrapWorkItemAsTask(IWorkItem todo, ISchedulingContext context, TaskScheduler sched)
        {
            var task = new Task(state => RunWorkItemTask(todo, sched), context);
            return task;
        }

        internal static Task WrapWorkItemWithPriorityAsTask(IWorkItem todo, PriorityContext context, TaskScheduler sched)
        {
            var task = new Task(state => RunWorkItemTaskWithPriority(todo, sched, context), context);
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

        private static void RunWorkItemTaskWithPriority(IWorkItem todo, TaskScheduler sched, PriorityContext context)
        {
            try
            {
                RuntimeContext.SetExecutionContext(todo.SchedulingContext, sched);
                todo.Execute(context);
            }
            finally
            {
                RuntimeContext.ResetExecutionContext();
            }
        }
    }
}
