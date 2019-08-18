package simpledb;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {

    /**
     * class for a page's lock, either shared or exclusive.
     */
    public static class PageLock {
        public final PageId pid;
        private Permissions perm;
        private int holdNum;

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

    /**
     * Map associates each transaction with its Lock.
     */
    private final ConcurrentHashMap<TransactionId, Set<PageLock>> txToLock;
    private final ConcurrentHashMap<PageId, Set<TransactionId>> lockTotx;

    public LockManager() {
        txToLock = new ConcurrentHashMap<>();
        lockTotx = new ConcurrentHashMap<>();
    }

    /**
     * Grant a lock based on transaction, many thread may represent the same tid
     */
    public synchronized void grantLock(TransactionId tid, PageId pid, Permissions perm) {
        Set<PageLock> lockSet = txToLock.get(tid);
        Set<TransactionId> txSet = lockTotx.get(pid);
        if (perm == Permissions.READ_ONLY) {
            if (lockSet != null) {
                if (lockSet.contains(new PageLock(pid, Permissions.READ_ONLY))) {
                    /* this transaction already holds a lock for this page */
                    return;
                }
                if (txSet != null && txSet.size() > 0) {
                    /* this page's lock is held by other txs */
                    Iterator<TransactionId> it1 = txSet.iterator();
                    Set<PageLock> ss = txToLock.get(it1.next());
                    PageLock lock = null;
                    for (PageLock p : ss) {
                        if (p.equals(new PageLock(pid, perm))) {
                            lock = p;
                            break;
                        }
                    }
                    /* try to get the read lock */
                    if (lock.perm != Permissions.READ_ONLY) {
                        while (lock.holdNum != 0) {
                            try {
                                lock.wait();
                            } catch (InterruptedException e) {

                            }
                        }
                        lock.holdNum++;
                    }

                    /* update map */
                    txSet.add(tid);
                    lockSet.add(lock);
                } else {
                    /* this page's lock isn't held by any tx */
                    PageLock lock = new PageLock(pid, perm);
                    lockSet.add(lock);
                    HashSet<TransactionId> s = new HashSet<>();
                    s.add(tid);
                    lockTotx.put(pid, s);
                }
            } else {
                if (txSet != null && txSet.size() > 0) {
                    /* this page's lock is held by other txs */
                    Iterator<TransactionId> it1 = txSet.iterator();
                    Iterator<PageLock> it2 = txToLock.get(it1.next()).iterator();
                    PageLock lock = it2.next();
                    /* try to get the read lock */
                    if (lock.perm != Permissions.READ_ONLY) {
                        while (lock.holdNum != 0) {
                            try {
                                this.wait();
                            } catch (InterruptedException e) {

                            }
                        }
                        lock.holdNum++;
                    }
                    /* update map */
                    txSet.add(tid);
                    HashSet<PageLock> s = new HashSet<>();
                    s.add(lock);
                    txToLock.put(tid, s);
                    ;
                } else {
                    /* this page's lock isn't held by any tx */
                    PageLock lock = new PageLock(pid, perm);
                    HashSet<TransactionId> ss = new HashSet<>();
                    ss.add(tid);
                    lockTotx.put(pid, ss);
                    HashSet<PageLock> s = new HashSet<>();
                    s.add(lock);
                    txToLock.put(tid, s);
                }
            }
        } else {
            if (txSet != null && txSet.size() > 0) {
                Iterator<TransactionId> it1 = txSet.iterator();
                Set<PageLock> ss = txToLock.get(it1.next());
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
                    } else {
                        releaseLock(tid, pid);
                    }
                }
                /* try to get the write lock */
                while (lock.holdNum != 0) {
                    try {
                        this.wait();
                    } catch (InterruptedException e) {

                    }
                }
                lock.holdNum++;
                lock.perm = Permissions.READ_WRITE;

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
                HashSet<TransactionId> ss = new HashSet<>();
                ss.add(tid);
                lockTotx.put(pid, ss);
            }
        }

    }

    public synchronized boolean holdLock(TransactionId tid, PageId pid) {
        /* compare based on PageId, permission doesn't matter here */
        return txToLock.containsKey(tid) && txToLock.get(tid).contains(new PageLock(pid, Permissions.READ_ONLY));
    }

    public synchronized void releaseLock(TransactionId tid, PageId pid) {
        if (holdLock(tid, pid)) {
            /* compare based on PageId, permission doesn't matter here */
            Set<TransactionId> txSet = lockTotx.get(pid);
            Iterator<TransactionId> it1 = txSet.iterator();
            Set<PageLock> ss = txToLock.get(it1.next());
            PageLock lock = null;
            for (PageLock p : ss) {
                if (p.equals(new PageLock(pid, Permissions.READ_WRITE))) {
                    lock = p;
                    break;
                }
            }
            txToLock.get(tid).remove(new PageLock(pid, Permissions.READ_WRITE));
            lockTotx.get(pid).remove(tid);
            lock.holdNum--;
            this.notifyAll();
        }
    }

    public synchronized Set<PageLock> getPages(TransactionId tid) {
        return txToLock.getOrDefault(tid, Collections.emptySet());
    }

}
