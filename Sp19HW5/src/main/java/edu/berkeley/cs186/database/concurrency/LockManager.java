package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.BaseTransaction;
import edu.berkeley.cs186.database.Database;

import java.util.*;

/**
 * LockManager maintains the bookkeeping for what transactions have
 * what locks on what resources. The lock manager should generally **not**
 * be used directly: instead, code should call methods of LockContext to
 * acquire/release/promote/escalate locks.
 *
 * The LockManager is primarily concerned with the mappings between
 * transactions, resources, and locks, and does not concern itself with
 * multiple levels of granularity (you can and should treat ResourceName
 * as a generic Object, rather than as an object encapsulating levels of
 * granularity, in this class).
 *
 * It follows that LockManager should allow **all**
 * requests that are valid from the perspective of treating every resource
 * as independent objects, even if they would be invalid from a
 * multigranularity locking perspective. For example, if LockManager#acquire
 * is called asking for an X lock on Table A, and the transaction has no
 * locks at the time, the request is considered valid (because the only problem
 * with such a request would be that the transaction does not have the appropriate
 * intent locks, but that is a multigranularity concern).
 *
 * Each resource the lock manager manages has its own queue of LockRequest objects
 * representing a request to acquire (or promote/acquire-and-release) a lock that
 * could not be satisfied at the time. This queue should be processed every time
 * a lock on that resource gets released, starting from the first request, and going
 * in order until a request cannot be satisfied. Requests taken off the queue should
 * be treated as if that transaction had made the request right after the resource was
 * released in absence of a queue (i.e. removing a request by T1 to acquire X(db) should
 * be treated as if T1 had just requested X(db) and there were no queue on db: T1 should
 * be given the X lock on db, and put in an unblocked state via BaseTransaction#unblock).
 *
 * This does mean that in the case of:
 *    queue: S(A) X(A) S(A)
 * only the first request should be removed from the queue when the queue is processed.
 */
public class LockManager {
    // transactionLocks is a mapping from transaction number to a list of lock
    // objects held by that transaction.
    private Map<Long, List<Lock>> transactionLocks = new HashMap<>();
    // resourceEntries is a mapping from resource names to a ResourceEntry
    // object, which contains a list of Locks on the object, as well as a
    // queue for requests on that resource.
    private Map<ResourceName, ResourceEntry> resourceEntries = new HashMap<>();

    // A ResourceEntry contains the list of locks on a resource, as well as
    // the queue for requests for locks on the resource.
    private class ResourceEntry {
        // List of currently granted locks on the resource.
        List<Lock> locks = new ArrayList<>();
        // Queue for yet-to-be-satisfied lock requests on this resource.
        Deque<LockRequest> waitingQueue = new ArrayDeque<>();

        // You may add helper methods here if you wish
        boolean notCompatible(LockType newLockType, BaseTransaction transaction) {
            for (Lock lock: locks){
                if (lock.transactionNum != transaction.getTransNum()
                        && !LockType.compatible(lock.lockType, newLockType)){
                    return true;
                }
            }
            return false;
        }

        boolean isQueued() {
            return !waitingQueue.isEmpty();
        }

        void addToQueueEnd(LockRequest newRequest){
            waitingQueue.addLast(newRequest);
        }

        void addToQueueFront(LockRequest newRequest) {waitingQueue.addFirst(newRequest);}


    }

    // You should not modify or use this directly.
    protected Map<Object, LockContext> contexts = new HashMap<>();

    /**
     * Helper method to fetch the resourceEntry corresponding to NAME.
     * Inserts a new (empty) resourceEntry into the map if no entry exists yet.
     */
    private ResourceEntry getResourceEntry(ResourceName name) {
        resourceEntries.putIfAbsent(name, new ResourceEntry());
        return resourceEntries.get(name);
    }

    // You may add helper methods here if you wish

    private boolean containLock(BaseTransaction transaction, ResourceName name){
        transactionLocks.putIfAbsent(transaction.getTransNum(), new LinkedList<>());
        List<Lock> lockList = transactionLocks.get(transaction.getTransNum());
        for (Lock lock : lockList) {
            if (lock.name.equals(name)){
                return true;
            }
        }
        return false;
    }

    private boolean containLockNotRelease(BaseTransaction transaction, ResourceName name,
                                          List<ResourceName> toRelease) {
        transactionLocks.putIfAbsent(transaction.getTransNum(), new LinkedList<>());
        List<Lock> lockList = transactionLocks.get(transaction.getTransNum());
        for (Lock lock : lockList) {
            if (lock.name.equals(name) && !toRelease.contains(name)){
                return true;
            }
        }
        return false;
    }

    private boolean isUpgrade(BaseTransaction transaction, Lock newLock) {
        for (Lock lock : transactionLocks.get(transaction.getTransNum())) {
            if (newLock.name.equals(lock.name) && newLock.transactionNum == lock.transactionNum) {
                ResourceEntry entry = getResourceEntry(newLock.name);
                entry.locks.remove(lock);
                entry.locks.add(newLock);
                lock.lockType = newLock.lockType;
                return true;
            }
        }
        return false;
    }

    /**
     * Acquire a LOCKTYPE lock on NAME, for transaction TRANSACTION, and releases all locks
     * in RELEASELOCKS after acquiring the lock, in one atomic action.
     *
     * Error checking must be done before any locks are acquired or released. If the new lock
     * is not compatible with another transaction's lock on the resource, the transaction is
     * blocked and the request is placed at the **front** of ITEM's queue.
     *
     * Locks in RELEASELOCKS should be released only after the requested lock has been acquired.
     * The corresponding queues should be processed.
     *
     * An acquire-and-release that releases an old lock on NAME **should not** change the
     * acquisition time of the lock on NAME, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), acquire X(A) and release S(A), the
     * lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if a lock on NAME is held by TRANSACTION and
     * isn't being released
     * @throws NoLockHeldException if no lock on a name in RELEASELOCKS is held by TRANSACTION
     */
    public void acquireAndRelease(BaseTransaction transaction, ResourceName name,
                                  LockType lockType, List<ResourceName> releaseLocks)
    throws DuplicateLockRequestException, NoLockHeldException {
        boolean blockTrans = false;
        synchronized (this) {
            //error checking for acquire and release
            if (containLockNotRelease(transaction, name, releaseLocks) ){
                throw new DuplicateLockRequestException("Duplicate lock on " + name);
            }

            LinkedList<Lock> releaseLockObjects = new LinkedList<>();
            for (ResourceName releaseName: releaseLocks) {
                if (!containLock(transaction, releaseName)) {
                    throw new NoLockHeldException("No lock to be released!");
                } else {
                    Iterator<Lock> lockList = transactionLocks.get(transaction.getTransNum()).iterator();
                    boolean found = false;
                    while (!found && lockList.hasNext()) {
                        Lock nextLock = lockList.next();
                        if (nextLock.name == releaseName) {
                            releaseLockObjects.addLast(nextLock);
                            found = true;
                        }
                    }
                }
            }

            //acquire
            ResourceEntry entry = getResourceEntry(name);
            Lock newLock = new Lock(name, lockType, transaction.getTransNum());
            if (entry.notCompatible(lockType, transaction)){
                entry.addToQueueFront(new LockRequest(transaction, newLock, releaseLockObjects));
                blockTrans = true;
            } else {
                //acquire success, then release
                if (!isUpgrade(transaction, newLock)) {
                    entry.locks.add(newLock);
                    transactionLocks.get(transaction.getTransNum()).add(newLock);
                }

                for (ResourceName releaseName: releaseLocks) {
                    if (releaseName != name) {
                        entry = getResourceEntry(releaseName);
                        boolean found = false;
                        //find corresponding lock and remove it
                        for(int i = 0; i < entry.locks.size() && !found; i++) {
                            if (entry.locks.get(i).transactionNum == transaction.getTransNum()) {
                                Lock toRemove = entry.locks.remove(i);
                                transactionLocks.get(transaction.getTransNum()).remove(toRemove);
                                found = true;
                            }
                        }

                        //process the queue
                        boolean isCompatible = true;
                        while (entry.isQueued() && isCompatible) {
                            LockRequest nextRequest = entry.waitingQueue.peek();
                            if (entry.notCompatible(nextRequest.lock.lockType, nextRequest.transaction)) {
                                isCompatible = false;
                            } else {
                                entry.waitingQueue.pop();
                                if (!isUpgrade(transaction, nextRequest.lock)) {
                                    entry.locks.add(nextRequest.lock);
                                    transactionLocks.get(nextRequest.transaction.getTransNum()).add(nextRequest.lock);
                                }
                                while (!nextRequest.releasedLocks.isEmpty()) {
                                    Lock nextToRemove = nextRequest.releasedLocks.remove(0);
                                    release(nextRequest.transaction, nextToRemove.name);
                                }
                                nextRequest.transaction.unblock();
                            }
                        }
                    }
                }
            }
        }

        if (blockTrans) {
            transaction.block();
        }
    }

    /**
     * Acquire a LOCKTYPE lock on NAME, for transaction TRANSACTION.
     *
     * Error checking must be done before the lock is acquired. If the new lock
     * is not compatible with another transaction's lock on the resource, or if there are
     * other transaction in queue for the resource, the transaction is
     * blocked and the request is placed at the **back** of NAME's queue.
     *
     * @throws DuplicateLockRequestException if a lock on NAME is held by
     * TRANSACTION
     */
    public void acquire(BaseTransaction transaction, ResourceName name,
                        LockType lockType) throws DuplicateLockRequestException {
        // You may modify any part of this method. You are not required to keep all your
        // code within the given synchronized block -- in fact,
        // you will have to write some code outside the synchronized block to avoid locking up
        // the entire lock manager when a transaction is blocked. You are also allowed to
        // move the synchronized block elsewhere if you wish.
        //throw new UnsupportedOperationException("TODO(hw5_part1): implement");

        boolean blockTrans = false;

        synchronized (this) {
            if (containLock(transaction, name)){
                throw new DuplicateLockRequestException("Duplicate lock on " + name);
            }

            ResourceEntry entry = getResourceEntry(name);
            Lock newLock = new Lock(name, lockType, transaction.getTransNum());
            if (entry.notCompatible(lockType, transaction) || entry.isQueued()){
                entry.addToQueueEnd(new LockRequest(transaction, newLock));
                blockTrans = true;
            } else {
                entry.locks.add(newLock);
                transactionLocks.get(transaction.getTransNum()).add(newLock);
            }
        }

        if (blockTrans) {
            transaction.block();
        }

    }

    /**
     * Release TRANSACTION's lock on NAME.
     *
     * Error checking must be done before the lock is released.
     *
     * NAME's queue should be processed after this call. If any requests in
     * the queue have locks to be released, those should be released, and the
     * corresponding queues also processed.
     *
     * @throws NoLockHeldException if no lock on NAME is held by TRANSACTION
     */
    public void release(BaseTransaction transaction, ResourceName name)
    throws NoLockHeldException {
        // You may modify any part of this method.
        synchronized (this) {
            if (!containLock(transaction, name)) {
                throw new NoLockHeldException("No lock to be released!");
            }

            ResourceEntry entry = getResourceEntry(name);

            //find corresponding lock and remove it
            int i = 0;
            boolean found = false;
            while (!found) {
                if (entry.locks.get(i).name.equals(name)
                        && entry.locks.get(i).transactionNum == transaction.getTransNum()) {
                    found = true;
                } else {
                    i++;
                }
            }
            Lock toRemove = entry.locks.remove(i);
            transactionLocks.get(transaction.getTransNum()).remove(toRemove);

            //process the queue
            boolean isCompatible = true;
            while (entry.isQueued() && isCompatible) {
                LockRequest nextRequest = entry.waitingQueue.peek();
                if (entry.notCompatible(nextRequest.lock.lockType, nextRequest.transaction)) {
                    isCompatible = false;
                } else {
                    entry.waitingQueue.pop();

                    entry.locks.add(nextRequest.lock);
                    transactionLocks.get(nextRequest.transaction.getTransNum()).add(nextRequest.lock);
                    while (!nextRequest.releasedLocks.isEmpty()) {
                        Lock nextToRemove = nextRequest.releasedLocks.remove(0);
                        release(nextRequest.transaction, nextToRemove.name);
                    }
                    nextRequest.transaction.unblock();
                }
            }
        }
    }

    /**
     * Promote TRANSACTION's lock on NAME to NEWLOCKTYPE (i.e. change TRANSACTION's lock
     * on NAME from the current lock type to NEWLOCKTYPE, which must be strictly more
     * permissive).
     *
     * Error checking must be done before any locks are changed. If the new lock
     * is not compatible with another transaction's lock on the resource, the transaction is
     * blocked and the request is placed at the **front** of ITEM's queue.
     *
     * A lock promotion **should not** change the acquisition time of the lock, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), promote X(A), the
     * lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if TRANSACTION already has a
     * NEWLOCKTYPE lock on NAME
     * @throws NoLockHeldException if TRANSACTION has no lock on NAME
     * @throws InvalidLockException if the requested lock type is not a promotion. A promotion
     * from lock type A to lock type B is valid if and only if B is substitutable
     * for A, and B is not equal to A.
     */
    public void promote(BaseTransaction transaction, ResourceName name,
                        LockType newLockType)
    throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // You may modify any part of this method.
        boolean blockTrans = false;
        synchronized (this) {
            //error checking
            Lock oldLock = null;
            for (Lock lock: transactionLocks.getOrDefault(transaction.getTransNum(), new LinkedList<>())) {
                if (lock.name.equals(name)) {
                    if (lock.lockType == newLockType) {
                        throw new DuplicateLockRequestException("Same LockType, no need to update!");
                    } else if (!LockType.substitutable(newLockType, lock.lockType)) {
                        throw new InvalidLockException("Not substitutable");
                    } else {
                        oldLock = lock;
                    }
                }
            }

            if (oldLock == null) {
                throw new NoLockHeldException("Lock doesn't exist!");
            }

            //update the lock
            ResourceEntry entry = getResourceEntry(name);
            Lock newLock = new Lock(name, newLockType, transaction.getTransNum());
            if (entry.notCompatible(newLockType, transaction)) {
                List<Lock> releaseLocks = new LinkedList<>();
                releaseLocks.add(oldLock);
                entry.addToQueueFront(new LockRequest(transaction, newLock, releaseLocks));
                blockTrans = true;
            } else {
                entry.locks.remove(oldLock);
                entry.locks.add(newLock);
                transactionLocks.get(transaction.getTransNum()).remove(oldLock);
                transactionLocks.get(transaction.getTransNum()).add(newLock);
            }
        }

        if (blockTrans) {
            transaction.block();
        }
    }

    /**
     * Return the type of lock TRANSACTION has on NAME (return NL if no lock is held).
     */
    public synchronized LockType getLockType(BaseTransaction transaction, ResourceName name) {
        //throw new UnsupportedOperationException("TODO(hw5_part1): implement");
        for (Lock lock : transactionLocks.get(transaction.getTransNum())) {
            if (lock.name.equals(name)) {
                return lock.lockType;
            }
        }
        return LockType.NL;
    }

    /**
     * Returns the list of locks held on NAME, in order of acquisition.
     * A promotion or acquire-and-release should count as acquired
     * at the original time.
     */
    public synchronized List<Lock> getLocks(ResourceName name) {
        return new ArrayList<>(resourceEntries.getOrDefault(name, new ResourceEntry()).locks);
    }

    /**
     * Returns the list of locks locks held by
     * TRANSACTION, in order of acquisition. A promotion or
     * acquire-and-release should count as acquired at the original time.
     */
    public synchronized List<Lock> getLocks(BaseTransaction transaction) {
        return new ArrayList<>(transactionLocks.getOrDefault(transaction.getTransNum(),
                               Collections.emptyList()));
    }

    /**
     * Create a lock context for the database. See comments at
     * the top of this file and the top of LockContext.java for more information.
     */
    public synchronized LockContext databaseContext() {
        contexts.putIfAbsent("database", new LockContext(this, null, "database"));
        return contexts.get("database");
    }

    /**
     * Create a lock context with no parent. Cannot be called "database".
     */
    public synchronized LockContext orphanContext(Object name) {
        if (name.equals("database")) {
            throw new IllegalArgumentException("cannot create orphan context named 'database'");
        }
        contexts.putIfAbsent(name, new LockContext(this, null, name));
        return contexts.get(name);
    }
}
