package org.sibench.capture;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * One session's access tracking, run ahead of the merge on a thread of its own. Which events
 * belong to which request and access unit depends only on the session's own events, and was most
 * of the merge's ordered thread; the merge still makes every decision that depends on the global
 * order (read points, versions, work units), by replaying the tracker's listener calls in the
 * places the tracker made them ({@link Tracked}).
 *
 * <p>A transaction-sized chunk gets a fresh tracker that continues the session's capture detail,
 * as the merge used to give it; a chunk ends with its commit or abort event. Objects the tracker
 * keeps changing after a call are passed with their state at the call: a unit's BitmapAnd group
 * when it starts (the group can bind it later) and a group's members when it forms (later units
 * join it). Units and requests do not change once settled or finished.
 */
final class SessionTracking implements SessionMerge.Items {
    /** Events tracked ahead of the merge per session. */
    static final int AHEAD = 4096;

    /** A listener call of the tracker, with the state the merge needs of what changes later. */
    record Call(
            int kind,
            AccessUnits.Request request,
            AccessUnits.Unit unit,
            AccessUnits.BitmapGroup group,
            List<AccessUnits.Unit> members,
            boolean first) {
        static final int REQUEST_STARTED = 0,
                REQUEST_FINISHED = 1,
                UNIT_STARTED = 2,
                UNIT_SETTLED = 3,
                GROUP_FORMED = 4,
                GROUP_SETTLED = 5;
    }

    /**
     * One event as the tracker routed it: whether it was tracked, the calls it made on the way
     * and the failure that stopped it. A chunk's last event also carries the chunk's end: the
     * calls ending the session part made, its failure, the capture detail and the tracker itself
     * for its counters.
     */
    static final class Tracked {
        final Event event;
        boolean tracked;
        final List<Call> calls = new ArrayList<>(2);
        Throwable failure;
        List<Call> endCalls;
        Throwable endFailure;
        AccessUnits.Detail detail;
        AccessUnits tracker;

        Tracked(Event event) {
            this.event = event;
        }
    }

    /** Marks the end of the session's items; never an item. */
    private static final Object END = new Object();

    private final ParsedTrace events;
    private final BlockingQueue<Object> ahead = new ArrayBlockingQueue<>(AHEAD + 2);
    private final Thread thread;
    private boolean ended;

    SessionTracking(ParsedTrace events) {
        this.events = events;
        thread = new Thread(this::run, "tracker");
        thread.setDaemon(true);
        thread.start();
    }

    /** Records each listener call into the current event's list. */
    private static final class Recorder implements AccessUnits.Listener {
        List<Call> target;

        @Override
        public void requestStarted(AccessUnits.Request request, Event event) {
            target.add(new Call(Call.REQUEST_STARTED, request, null, null, null, false));
        }

        @Override
        public void requestFinished(AccessUnits.Request request) {
            target.add(new Call(Call.REQUEST_FINISHED, request, null, null, null, false));
        }

        @Override
        public void unitStarted(AccessUnits.Unit unit, Event event) {
            var group = unit.group;
            target.add(new Call(Call.UNIT_STARTED, null, unit, group, null, group != null && group.members.get(0) == unit));
        }

        @Override
        public void unitSettled(AccessUnits.Unit unit) {
            target.add(new Call(Call.UNIT_SETTLED, null, unit, unit.group, null, false));
        }

        @Override
        public void groupFormed(AccessUnits.BitmapGroup group) {
            target.add(new Call(Call.GROUP_FORMED, null, null, group, List.copyOf(group.members), false));
        }

        @Override
        public void groupSettled(AccessUnits.BitmapGroup group) {
            target.add(new Call(Call.GROUP_SETTLED, null, null, group, null, false));
        }
    }

    private void run() {
        var recorder = new Recorder();
        AccessUnits tracker = null;
        AccessUnits.Detail detail = null;
        try {
            while (true) {
                Event e = events.next();
                if (e == null) break;
                if (tracker == null) {
                    tracker = new AccessUnits(recorder);
                    if (detail == null) tracker.beginSession();
                    else tracker.continueSession(detail);
                }
                var item = new Tracked(e);
                recorder.target = item.calls;
                try {
                    item.tracked = tracker.accept(e);
                } catch (IOException | RuntimeException failure) {
                    // The tracker's state is undefined after a failure: nothing follows it.
                    item.failure = failure;
                    ahead.put(item);
                    break;
                }
                String type = e.type();
                if (type.equals("transaction_commit") || type.equals("transaction_abort")) {
                    item.endCalls = new ArrayList<>();
                    recorder.target = item.endCalls;
                    detail = tracker.detail();
                    item.detail = detail;
                    try {
                        tracker.endSession();
                    } catch (IOException | RuntimeException failure) {
                        item.endFailure = failure;
                    }
                    item.tracker = tracker;
                    tracker = null;
                    ahead.put(item);
                    if (item.endFailure != null) break;
                    continue;
                }
                ahead.put(item);
            }
        } catch (InterruptedException closed) {
            Thread.currentThread().interrupt();
            return;
        } catch (IOException | RuntimeException failure) {
            // A trace that fails to read fails where the merge reaches it.
            try {
                ahead.put(failure);
            } catch (InterruptedException closed) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        try {
            ahead.put(END);
        } catch (InterruptedException closed) {
            Thread.currentThread().interrupt();
        }
    }

    /** The next tracked event, or null after the last one. */
    @Override
    public Object next() throws IOException {
        if (ended) return null;
        Object next;
        try {
            next = ahead.take();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while tracking a session trace", interrupted);
        }
        if (next == END) {
            ended = true;
            return null;
        }
        if (next instanceof IOException failure) throw failure;
        if (next instanceof RuntimeException failure) throw failure;
        return next;
    }

    @Override
    public void close() {
        thread.interrupt();
        events.close();
    }
}
