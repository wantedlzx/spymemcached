package net.spy.memcached.util;

import java.util.Collection;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * As task queue specifically designed to run with a thread pool executor. The task queue is optimised to properly
 * utilize threads within a thread pool executor. If you use a normal queue, the executor will spawn threads when there
 * are idle threads and you wont be able to force items onto the queue itself.
 */
public class TaskQueue<T> extends LinkedBlockingQueue<T> {

  private static final long serialVersionUID = -8492534811742453555L;

  private ExtendableThreadPoolExecutor parent;

  public TaskQueue() {
    super();
  }

  public TaskQueue(int capacity) {
    super(capacity);
  }

  public TaskQueue(Collection<? extends T> c) {
    super(c);
  }

  public void setParent(ExtendableThreadPoolExecutor tp) {
    parent = tp;
  }

  public boolean force(T o) {
    if (parent.isShutdown()) {
      throw new RejectedExecutionException("Executor not running, can't force a command into the queue");
    }

    return super.offer(o); // forces the item onto the queue, to be used if the task is rejected
  }

  public boolean force(T o, long timeout, TimeUnit unit) throws InterruptedException {
    if (parent.isShutdown()) {
      throw new RejectedExecutionException("Executor not running, can't force a command into the queue");
    }

    return super.offer(o, timeout, unit); // forces the item onto the queue, to be used if the task is rejected
  }

  @Override
  public  boolean offer(T o) {
    int currentPoolSize = parent.getPoolSize();

    // we are maxed out on threads, simply queue the object
    if (currentPoolSize == parent.getMaximumPoolSize()) {
      return super.offer(o);
    }

    // we have idle threads, just add it to the queue
    if (parent.getSubmittedCount() < currentPoolSize) {
      return super.offer(o);
    }

    // if we have less threads than maximum force creation of a new thread
    if (currentPoolSize < parent.getMaximumPoolSize()) {
      return false;
    }

    // if we reached here, we need to add it to the queue
    return super.offer(o);
  }
}