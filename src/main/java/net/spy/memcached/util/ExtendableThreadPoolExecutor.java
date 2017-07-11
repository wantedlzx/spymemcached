package net.spy.memcached.util;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ExtendableThreadPoolExecutor extends ThreadPoolExecutor {

  /**
   * The number of tasks submitted but not yet finished. This includes tasks
   * in the queue and tasks that have been handed to a worker thread but the
   * latter did not start executing the task yet.
   * This number is always greater or equal to {@link #getActiveCount()}.
   */
  private final AtomicInteger submittedCount = new AtomicInteger(0);

  public ExtendableThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, TaskQueue<Runnable> workQueue, ThreadFactory threadFactory) {
    super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, new RejectHandler());
    workQueue.setParent(this);
  }


  @Override
  protected void afterExecute(Runnable r, Throwable t) {
    submittedCount.decrementAndGet();
  }


  public int getSubmittedCount() {
    return submittedCount.get();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void execute(Runnable command) {
    execute(command,0,TimeUnit.MILLISECONDS);
  }

  /**
   * Executes the given command at some time in the future.  The command
   * may execute in a new thread, in a pooled thread, or in the calling
   * thread, at the discretion of the <tt>Executor</tt> implementation.
   * If no threads are available, it will be added to the work queue.
   * If the work queue is full, the system will wait for the specified
   * time and it throw a RejectedExecutionException if the queue is still
   * full after that.
   *
   * @param command the runnable task
   * @param timeout A timeout for the completion of the task
   * @param unit The timeout time unit
   * @throws RejectedExecutionException if this task cannot be
   * accepted for execution - the queue is full
   * @throws NullPointerException if command or unit is null
   */
  public void execute(Runnable command, long timeout, TimeUnit unit) {
    submittedCount.incrementAndGet();

    try {
      super.execute(command);
    } catch (RejectedExecutionException rx) {
      if (super.getQueue() instanceof TaskQueue) {
        final TaskQueue<Runnable> queue = (TaskQueue<Runnable>)super.getQueue();
          try {
            if (!queue.force(command, timeout, unit)) {
              submittedCount.decrementAndGet();

              throw new RejectedExecutionException("Queue capacity is full.");
            }
          } catch (InterruptedException x) {
            submittedCount.decrementAndGet();

            throw new RejectedExecutionException(x);
          }
      } else {
        submittedCount.decrementAndGet();
        throw rx;
      }
    }
  }

  private static class RejectHandler implements RejectedExecutionHandler {
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
      throw new RejectedExecutionException();
    }
  }
}