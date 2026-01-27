package org.apache.hadoop.ozone.om.request.invocation;


import com.google.common.base.Preconditions;

class CallReturn {
  /** The return state. */
  enum State {
    /** Call is returned successfully. */
    RETURNED,
    /** Call throws an exception. */
    EXCEPTION,
    /** Call should be retried according to the {@link RetryPolicy}. */
    RETRY,
    /** Call should wait and then retry according to the {@link RetryPolicy}. */
    WAIT_RETRY,
    /** Call, which is async, is still in progress. */
    ASYNC_CALL_IN_PROGRESS,
    /** Call, which is async, just has been invoked. */
    ASYNC_INVOKED
  }

  static final CallReturn ASYNC_CALL_IN_PROGRESS = new CallReturn(
      CallReturn.State.ASYNC_CALL_IN_PROGRESS);
  static final CallReturn ASYNC_INVOKED = new CallReturn(CallReturn.State.ASYNC_INVOKED);
  static final CallReturn RETRY = new CallReturn(CallReturn.State.RETRY);
  static final CallReturn WAIT_RETRY = new CallReturn(CallReturn.State.WAIT_RETRY);

  private final Object returnValue;
  private final Throwable thrown;
  private final CallReturn.State state;

  CallReturn(Object r) {
    this(r, null, CallReturn.State.RETURNED);
  }
  CallReturn(Throwable t) {
    this(null, t, CallReturn.State.EXCEPTION);
    Preconditions.checkNotNull(t);
  }
  private CallReturn(CallReturn.State s) {
    this(null, null, s);
  }
  private CallReturn(Object r, Throwable t, CallReturn.State s) {
    Preconditions.checkArgument(r == null || t == null);
    returnValue = r;
    thrown = t;
    state = s;
  }

  CallReturn.State getState() {
    return state;
  }

  Object getReturnValue() throws Throwable {
    if (state == CallReturn.State.EXCEPTION) {
      throw thrown;
    }
    Preconditions.checkState(state == CallReturn.State.RETURNED, "state == %s", state);
    return returnValue;
  }
}
