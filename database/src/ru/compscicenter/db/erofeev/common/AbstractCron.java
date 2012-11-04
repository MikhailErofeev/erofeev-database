package ru.compscicenter.db.erofeev.common;

/**
 * Created with IntelliJ IDEA.
 * User: erofeev
 * Date: 11/4/12
 * Time: 11:12 AM
 * To change this template use File | Settings | File Templates.
 */
public abstract class AbstractCron implements Runnable {
    private long ms;

    protected AbstractCron(long ms) {
        this.ms = ms;
    }

    protected abstract void action();

    @Override
    public final void run() {
        while (true) {
            try {
                Thread.sleep(ms);
                action();
            } catch (InterruptedException ex) {
            }
        }
    }
}
