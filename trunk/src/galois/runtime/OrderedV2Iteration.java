package galois.runtime;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class OrderedV2Iteration extends Iteration {
  private final List<Object> items;
  private Object currentWorkItem;
  private final AtomicReference<Status> status;

  public OrderedV2Iteration(int id) {
    super(id);
    items = new ArrayList<Object>();
    status = new AtomicReference<Status>(Status.IN_FLIGHT);
  }

  public void addItem(Object item) {
    items.add(item);
  }

  public boolean isDone() {
    return status.get().isDone();
  }

  public List<Object> getItems() {
    return items;
  }

  public boolean isReadyToCommit() {
    return status.get() == Status.READY_TO_COMMIT;
  }

  public boolean isAborting() {
    return status.get() == Status.ABORTING;
  }

  public Object getCurrentWorkItem() {
    return currentWorkItem;
  }

  public void setCurrentWorkItem(Object obj) {
    currentWorkItem = obj;
  }

  public void setReadyToCommit() {
    assert status.get() == Status.IN_FLIGHT;
    status.set(Status.READY_TO_COMMIT);
  }

  @Override
  int performAbort() {
    // Make sure only one thread performs abort
    while (true) {
      Status cur = status.get();
      if (cur == Status.ABORTED)
        return 0;
      if (cur == Status.ABORTING)
        continue;
      if (status.compareAndSet(cur, Status.ABORTING)) {
        int retval = super.performAbort();
        status.set(Status.ABORTED);
        return retval;
      }
    }
  }

  @Override
  int performCommit(boolean releaseLocks) {
    int retval = super.performCommit(releaseLocks);
    currentWorkItem = null;
    assert status.get() == Status.READY_TO_COMMIT;
    status.set(Status.IN_FLIGHT);
    return retval;
  }

  @Override
  protected int clearLogs(boolean releaseLocks) {
    int retval = super.clearLogs(releaseLocks);
    items.clear();

    return retval;
  }

  /**
   * Status of an iteration.
   * 
   * <dl>
   * <dt>IN_FLIGHT
   * <dd>unscheduled or executing
   * <dt>READY_TO_COMMIT
   * <dd>finished executing and can commit
   * <dt>ABORTING
   * <dd>enforces mutual exclusion of thread trying to abort this iteration
   * <dt>ABORTED
   * <dd>finished executing and aborted
   * </dl>
   * 
   * The valid transitions (and who can make the transition):
   * 
   * <pre>
   *  [0] IN_FLIGHT -&gt; READY_TO_COMMIT (self)   // performAbort
   *  [1] IN_FLIGHT -&gt; ABORTING        (self)   //   "   "
   *  [2] READY_TO_COMMIT -&gt; ABORTING (anyone)  // performSynchronousAbort
   *  [3] ABORTING -&gt; ABORTED (whoever did [2]) // performAbort or performSynchronousAbort
   * </pre>
   */
  private enum Status {
    IN_FLIGHT, READY_TO_COMMIT, ABORTING, ABORTED;
    public boolean isDone() {
      return this == READY_TO_COMMIT || this == ABORTED;
    }
  }
}
