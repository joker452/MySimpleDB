package simpledb;

import java.security.PrivilegedExceptionAction;
import java.util.Set;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

public class LockManager {

    /**
     * class for a page's lock, either shared or exclusive.
     */
    public static class PageLock {
        public final PageId pid;
        public Permissions perm;
        public int holdNum;

        public PageLock(PageId pid, Permissions perm) {
            this.pid = pid;
            this.perm = perm;
            holdNum = 1;
        }

        public void upgradeLock() {
            if (perm == Permissions.READ_ONLY) {
                perm = Permissions.READ_WRITE;
            }
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || o.getClass() != this.getClass()) {
                return false;
            }

            if (this == o) {
                return true;
            }

            return this.pid.equals(((PageLock) o).pid);
        }

        @Override
        public int hashCode() {
            return pid.hashCode();
        }
    }

    static class Digraph {
        final ConcurrentHashMap<TransactionId, HashSet<TransactionId>> waitList;

        public Digraph() {
            waitList = new ConcurrentHashMap<>();
        }

        public void addVertex(TransactionId tid) {
            if (waitList.containsKey(tid)) {
                return;
            }
            waitList.put(tid, new HashSet<>());
        }

        public void addEdge(TransactionId from, TransactionId to) {
            addVertex(from);
            addVertex(to);
            waitList.get(from).add(to);
        }

        public void removeEdge(TransactionId from, TransactionId to) {
            if (waitList.containsKey(from) && waitList.containsKey(to)) {
                waitList.get(from).remove(to);
            }
        }

        public void removeVertex(TransactionId tid) {
            waitList.remove(tid);
        }

        private boolean isCyclicHelper(TransactionId id, ConcurrentHashMap<TransactionId, Boolean> visited,
                                       ConcurrentHashMap<TransactionId, Boolean> traceStack) {
            if (traceStack.getOrDefault(id, false)) {
                return true;
            }

            if (visited.getOrDefault(id, false)) {
                return false;
            }
            visited.put(id, true);
            traceStack.put(id, true);
            Set<TransactionId> s = waitList.get(id);

            for (TransactionId t : s)
                if (isCyclicHelper(t, visited, traceStack)) {
                    return true;
                }
            traceStack.put(id, false);
            return false;
        }

        public boolean isCyclic() {
            int v = waitList.size();
            ConcurrentHashMap<TransactionId, Boolean> visited = new ConcurrentHashMap<>();
            ConcurrentHashMap<TransactionId, Boolean> traceStack = new ConcurrentHashMap<>();
            for (TransactionId id : waitList.keySet())
                if (isCyclicHelper(id, visited, traceStack)) {
                    return true;
                }
            return false;
        }
    }

    /**
     * Map associates each transaction with its Lock.
     */
    final ConcurrentHashMap<TransactionId, Set<PageLock>> txToLock;
    final ConcurrentHashMap<PageId, Set<TransactionId>> lockTotx;
    final Digraph graph;

    public LockManager() {
        txToLock = new ConcurrentHashMap<>();
        lockTotx = new ConcurrentHashMap<>();
        graph = new Digraph();
    }

    /**
     * Grant a lock based on transaction, many thread may represent the same tid
     */
    public synchronized void grantLock(TransactionId tid, PageId pid,
                                       Permissions perm) throws TransactionAbortedException {
        Set<PageLock> lockSet = txToLock.get(tid);
        Set<TransactionId> txSet = lockTotx.get(pid);
        if (perm.equals(Permissions.READ_ONLY)) {
            /* request a read lock */
            if (txSet != null) {
                /* there is some tx hold this lock */
                if (lockSet != null && lockSet.contains(new PageLock(pid, Permissions.READ_ONLY))) {
                    /* this transaction already holds a lock for this page */
                    return;
                }
                /*
                this page's lock is held by other txs
                we need to get the lock through any of its holder,
                so choose one from all of its current holder
                */
                Iterator<TransactionId> it = txSet.iterator();
                Set<PageLock> ss = txToLock.get(it.next());
                PageLock lock = null;
                for (PageLock p : ss) {
                    if (p.equals(new PageLock(pid, perm))) {
                        lock = p;
                        break;
                    }
                }

                /* try to get the read lock */
                if (lock.perm != Permissions.READ_ONLY) {
                    /* add to wait graph, abort if there will be a cycle after allow this tx to wait*/
                    for (TransactionId id : txSet) {
                        graph.addEdge(tid, id);
                    }
                    if (graph.isCyclic()) {
                        /* deadlock will occur */
                        for (TransactionId id : txSet) {
                            graph.removeEdge(tid, id);
                        }
                        graph.removeVertex(tid);
                        if (lockSet != null) {
                            for (PageLock id: lockSet) {
                                releaseLock(tid, id.pid);
                            }
                        }
                        throw new TransactionAbortedException();
                    }
                    while (lock.holdNum != 0) {
                        try {
                            this.wait();
                        } catch (InterruptedException e) {

                        }
                    }
                }
                graph.removeVertex(tid);
                lock.holdNum++;
                /* update map */
                if (lockSet == null) {
                    /* this tx doesn't hold any lock */
                    HashSet<PageLock> s = new HashSet<>();
                    s.add(lock);
                    txToLock.put(tid, s);
                } else {
                    lockSet.add(lock);
                }
                txSet.add(tid);
            } else {
                /*
                this lock isn't held by any tx, so allocate
                a new lock and add it to both maps
                */
                PageLock lock = new PageLock(pid, perm);
                if (lockSet == null) {
                    /* this tx doesn't hold any lock */
                    HashSet<PageLock> s = new HashSet<>();
                    s.add(lock);
                    txToLock.put(tid, s);
                } else {
                    lockSet.add(lock);
                }
                HashSet<TransactionId> s = new HashSet<>();
                s.add(tid);
                lockTotx.put(pid, s);
            }
        } else {
            /* request a write lock */
            if (txSet != null) {
                /*
                there is some tx holding this lock,
                find the lock
                 */
                Iterator<TransactionId> it = txSet.iterator();
                Set<PageLock> ss = txToLock.get(it.next());
                PageLock lock = null;
                for (PageLock p : ss) {
                    if (p.equals(new PageLock(pid, perm))) {
                        lock = p;
                        break;
                    }
                }
                if (txSet.contains(tid)) {
                    if (txSet.size() == 1) {
                        /* this transaction is the only transaction holds the lock for this page */
                        if (lock.perm == Permissions.READ_ONLY) {
                            lock.upgradeLock();
                            lock.holdNum = 1;
                        }
                        return;
                    }
//                    } else {
//                        /* release its read lock to make it possible to get a write lock */
//                        releaseLock(tid, pid);
//                    }
                }
                /* add to wait graph, abort if there will be a cycle after allow this tx to wait*/
                for (TransactionId id : txSet) {
                    graph.addEdge(tid, id);
                }
                if (graph.isCyclic()) {
                    /* deadlock will occur */
                    for (TransactionId id : txSet) {
                        graph.removeEdge(tid, id);
                    }
                    graph.removeVertex(tid);
                    if (lockSet != null) {
                        for (PageLock id: lockSet) {
                            releaseLock(tid, id.pid);
                        }
                    }
                    throw new TransactionAbortedException();
                }
                /* try to get the write lock */
                while (lock.holdNum != 0) {
                    try {
                        this.wait();
                    } catch (InterruptedException e) {

                    }
                }
                graph.removeVertex(tid);
                lock.holdNum++;
                lock.perm = Permissions.READ_WRITE;

                /* update map */
                txSet.add(tid);
                if (lockSet != null) {
                    lockSet.add(lock);
                } else {
                    HashSet<PageLock> s = new HashSet<>();
                    s.add(lock);
                    txToLock.put(tid, s);
                }
            } else {
                /* this page's lock isn't held by any tx */
                PageLock lock = new PageLock(pid, perm);
                if (lockSet != null) {
                    lockSet.add(lock);
                } else {
                    HashSet<PageLock> s = new HashSet<>();
                    s.add(lock);
                    txToLock.put(tid, s);
                }
                HashSet<TransactionId> s = new HashSet<>();
                s.add(tid);
                lockTotx.put(pid, s);
            }
        }

    }

    public synchronized boolean holdLock(TransactionId tid, PageId pid) {
        /* compare based on PageId, permission doesn't matter here */
        return lockTotx.containsKey(pid) && lockTotx.get(pid) != null
                && lockTotx.get(pid).contains(tid);
    }

    public synchronized void releaseLock(TransactionId tid, PageId pid) {
        if (holdLock(tid, pid)) {
//            System.out.println("release" + tid.getId());
            Set<PageLock> lockSet = txToLock.get(tid);
            Set<TransactionId> txSet = lockTotx.get(pid);
            PageLock lock = null;
            /* compare based on PageId, permission doesn't matter here */
            PageLock targetLock = new PageLock(pid, Permissions.READ_WRITE);
            for (PageLock l : lockSet) {
                if (l.equals(targetLock)) {
                    lock = l;
                }
            }
            lock.holdNum--;
            /* remove lock from two maps */
            txSet.remove(tid);
            if (txSet.size() == 0) {
                lockTotx.remove(pid);
            }
            lockSet.remove(lock);
            if (lockSet.size() == 0) {
                txToLock.remove(tid);
            }
            this.notifyAll();
        }
    }

    /**
     * get all the lock for the specified transaction
     */
    public synchronized Set<PageLock> getPages(TransactionId tid) {
        /* return a copy of the set to support modification when iterating */
        return new HashSet<>(txToLock.getOrDefault(tid, Collections.emptySet()));
    }

    /**
     * get all the transactions for the specified lock
     */
    public synchronized Set<TransactionId> getTxId(PageId pid) {
        return lockTotx.getOrDefault(pid, Collections.emptySet());
    }

}
