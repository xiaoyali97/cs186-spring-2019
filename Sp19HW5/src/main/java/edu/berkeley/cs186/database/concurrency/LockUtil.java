package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.BaseTransaction;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock acquisition
 * for the user (you, in the second half of Part 2). Generally speaking, you should use LockUtil
 * for lock acquisition instead of calling LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that TRANSACTION can perform actions requiring LOCKTYPE on LOCKCONTEXT.
     *
     * This method should promote/escalate as needed, but should only grant the least
     * permissive set of locks needed.
     *
     * lockType must be one of LockType.S, LockType.X, and behavior is unspecified
     * if an intent lock is passed in to this method (you can do whatever you want in this case).
     *
     * If TRANSACTION is null, this method should do nothing.
     */
    public static void ensureSufficientLockHeld(BaseTransaction transaction, LockContext lockContext,
            LockType lockType) {
        //throw new UnsupportedOperationException("TODO(hw5_part2): implement");
        if (transaction == null) {
            return;
        }

        if (LockType.substitutable(lockContext.getEffectiveLockType(transaction), lockType)) {
            return;
        }

        //ensure parent has permission
        LockType requiredParentLock = LockType.parentLock(lockType);
        LockContext parentContext = lockContext.parentContext();
        if (parentContext != null) {
            ensureSufficientLockHeld(transaction, parentContext, requiredParentLock);
        }

        //when got here, assume had all required parent locks
        //if NL, accquire; else promote
        if (lockContext.getEffectiveLockType(transaction).equals(LockType.NL)) {
            lockContext.acquire(transaction, lockType);
        } else {
            //try to promote
            try {
                lockContext.promote(transaction, lockType);
            } catch (InvalidLockException e){
                lockContext.escalate(transaction);
                ensureSufficientLockHeld(transaction, lockContext, lockType);
            }
        }
    }
}
