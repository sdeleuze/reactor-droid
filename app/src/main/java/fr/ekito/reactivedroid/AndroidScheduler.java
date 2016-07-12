package fr.ekito.reactivedroid;

import android.os.Handler;
import android.os.Looper;
import android.os.Message;

import java.util.concurrent.TimeUnit;

import reactor.core.flow.Cancellation;
import reactor.core.scheduler.TimedScheduler;

/**
 * Android Scheduler.
 */
public final class AndroidScheduler implements TimedScheduler {

    final Handler handler;

    static final AndroidScheduler MAIN_THREAD = new AndroidScheduler(Looper.getMainLooper());

    public static AndroidScheduler mainThread() {
        return MAIN_THREAD;
    }

    public AndroidScheduler(Handler handler) {
        this.handler = handler;
    }

    public AndroidScheduler(Looper looper) {
        this.handler = new Handler(looper);
    }

    @Override
    public Cancellation schedule(final Runnable task, long delay, TimeUnit unit) {

        handler.postDelayed(task, unit.toMillis(delay));

        return new Cancellation() {
            @Override
            public void dispose() {
                handler.removeCallbacks(task);
            }
        };
    }

    @Override
    public Cancellation schedulePeriodically(Runnable task, long initialDelay, long period, TimeUnit unit) {
        long nowMillis = now(TimeUnit.MILLISECONDS);
        long initialMillis = unit.toMillis(initialDelay);

        final PeriodicDirectTask pt = new PeriodicDirectTask(task, this, nowMillis + initialMillis, unit.toMillis(period));

        Message message = Message.obtain(handler, pt);
        message.obj = this; // mass cancellation token

        handler.sendMessageDelayed(message, initialMillis);

        return new Cancellation() {
            @Override
            public void dispose() {
                handler.removeCallbacks(pt);
            }
        };
    }

    @Override
    public Cancellation schedule(final Runnable task) {
        handler.post(task);

        return new Cancellation() {
            @Override
            public void dispose() {
                handler.removeCallbacks(task);
            }
        };
    }


    @Override
    public long now(TimeUnit unit) {
        return unit.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public TimedWorker createWorker() {
        return new AndroidWorker(handler);
    }

    @Override
    public void start() {

    }

    @Override
    public void shutdown() {

    }

    static final class AndroidWorker implements TimedWorker {

        final Handler handler;

        volatile boolean shutdown;

        public AndroidWorker(Handler handler) {
            this.handler = handler;
        }

        @Override
        public Cancellation schedule(final Runnable task, long delay, TimeUnit unit) {
            if (shutdown) {
                return REJECTED;
            }

            Message message = Message.obtain(handler, task);
            message.obj = this; // Used as token for unsubscription operation.

            handler.sendMessageDelayed(message, unit.toMillis(delay));

            if (shutdown) {
                handler.removeCallbacks(task);
                return REJECTED;
            }

            return new Cancellation() {
                @Override
                public void dispose() {
                    handler.removeCallbacks(task);
                }
            };
        }

        @Override
        public Cancellation schedulePeriodically(Runnable task, long initialDelay, long period, TimeUnit unit) {
            if (shutdown) {
                return REJECTED;
            }

            long nowMillis = now(TimeUnit.MILLISECONDS);
            long initialMillis = unit.toMillis(initialDelay);

            final PeriodicTask pt = new PeriodicTask(task, this, nowMillis + initialMillis, unit.toMillis(period));

            Message message = Message.obtain(handler, pt);
            message.obj = this; // mass cancellation token

            handler.sendMessageDelayed(message, initialMillis);

            if (shutdown) {
                handler.removeCallbacks(pt);
                return REJECTED;
            }

            return new Cancellation() {
                @Override
                public void dispose() {
                    handler.removeCallbacks(pt);
                }
            };
        }

        @Override
        public Cancellation schedule(final Runnable task) {
            if (shutdown) {
                return REJECTED;
            }

            Message message = Message.obtain(handler, task);
            message.obj = this; // mass cancellation token

            handler.sendMessage(message);

            if (shutdown) {
                handler.removeCallbacks(task);
                return REJECTED;
            }

            return new Cancellation() {
                @Override
                public void dispose() {
                    handler.removeCallbacks(task);
                }
            };
        }

        @Override
        public void shutdown() {
            shutdown = true;
            handler.removeCallbacksAndMessages(this);
        }

        @Override
        public long now(TimeUnit unit) {
            return unit.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        }

        static final class PeriodicTask implements Runnable, Cancellation {
            final Runnable task;

            final AndroidWorker worker;

            final long start;

            final long period;

            long count;

            volatile boolean cancelled;

            public PeriodicTask(Runnable task, AndroidWorker worker, long start, long period) {
                this.task = task;
                this.worker = worker;
                this.start = start;
                this.period = period;
            }

            @Override
            public void run() {
                if (cancelled || worker.shutdown) {
                    return;
                }

                task.run();

                if (cancelled || worker.shutdown) {
                    return;
                }

                long next = start + (++count) * period;
                long now = worker.now(TimeUnit.MILLISECONDS);

                long delta = next - now;

                Handler handler = worker.handler;

                Message message = Message.obtain(handler, this);
                message.obj = worker; // mass cancellation token

                if (delta <= 0L) {
                    handler.sendMessage(message);
                } else {
                    handler.sendMessageDelayed(message, delta);
                }

                if (cancelled || worker.shutdown) {
                    handler.removeCallbacks(this);
                    return;
                }
            }


            @Override
            public void dispose() {
                cancelled = true;
                worker.handler.removeCallbacks(this);
            }
        }
    }

    static final class PeriodicDirectTask implements Runnable, Cancellation {
        final Runnable task;

        final AndroidScheduler worker;

        final long start;

        final long period;

        long count;

        volatile boolean cancelled;

        public PeriodicDirectTask(Runnable task, AndroidScheduler worker, long start, long period) {
            this.task = task;
            this.worker = worker;
            this.start = start;
            this.period = period;
        }

        @Override
        public void run() {
            if (cancelled) {
                return;
            }

            task.run();

            if (cancelled) {
                return;
            }

            long next = start + (++count) * period;
            long now = worker.now(TimeUnit.MILLISECONDS);

            long delta = next - now;

            Handler handler = worker.handler;

            Message message = Message.obtain(handler, this);
            message.obj = worker; // mass cancellation token

            if (delta <= 0L) {
                handler.sendMessage(message);
            } else {
                handler.sendMessageDelayed(message, delta);
            }

            if (cancelled) {
                handler.removeCallbacks(this);
                return;
            }
        }


        @Override
        public void dispose() {
            cancelled = true;
            worker.handler.removeCallbacks(this);
        }


    }
}
