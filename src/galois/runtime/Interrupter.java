package galois.runtime;

import util.fn.Lambda0Void;

public class Interrupter implements Runnable {
  private boolean living = true;
  private final Lambda0Void callback;

  public Interrupter(Lambda0Void callback) {
    this.callback = callback;
  }

  @Override
  public void run() {
    try {
      while (living) {
        Thread.sleep(1);
        if (living) {
          callback.call();

        }
      }
    } catch (InterruptedException e) {
      throw new Error(e);
    }
  }

  public void start() {
    Thread t = new Thread(this);
    t.setDaemon(true);
    t.start();
  }

  public void stop() {
    living = false;
  }
}