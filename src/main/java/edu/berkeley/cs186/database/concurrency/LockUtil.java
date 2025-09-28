package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock
 * acquisition for the user (you, in the last task of Part 2). Generally
 * speaking, you should use LockUtil for lock acquisition instead of calling
 * LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring
     * `requestType` on `lockContext`.
     *
     * `requestType` is guaranteed to be one of: S, X, NL.
     *
     * This method should promote/escalate/acquire as needed, but should only
     * grant the least permissive set of locks needed. We recommend that you
     * think about what to do in each of the following cases:
     * - The current lock type can effectively substitute the requested type
     * - The current lock type is IX and the requested lock is S
     * - The current lock type is an intent lock
     * - None of the above: In this case, consider what values the explicit
     *   lock type can be, and think about how ancestor looks will need to be
     *   acquired or changed.
     *
     * You may find it useful to create a helper method that ensures you have
     * the appropriate locks on all ancestors.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType requestType) {
        // requestType must be S, X, or NL
        assert (requestType == LockType.S || requestType == LockType.X || requestType == LockType.NL);

        // Do nothing if the transaction or lockContext is null
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction == null || lockContext == null) return;

        // You may find these variables useful
        LockContext parentContext = lockContext.parentContext();
        LockType effectiveLockType = lockContext.getEffectiveLockType(transaction);
        LockType explicitLockType = lockContext.getExplicitLockType(transaction);

        // Case 1
        if (LockType.substitutable(effectiveLockType, requestType)) {
            return;
        }

        // Case 2
        if (effectiveLockType.equals(LockType.IX) && requestType.equals(LockType.S)) {
            // Ensure parent has IX/SIX lock before promotion
            if (!(
                    parentContext.getEffectiveLockType(transaction).equals(LockType.IX)
                            || parentContext.getEffectiveLockType(transaction).equals(LockType.SIX))
            ) {
                // add intent locks to parent
                addIntentLock(transaction, lockContext, LockType.IX);
            }
            // Promote to SIX
            lockContext.promote(transaction, LockType.SIX);
            return;
        }

        // Case 3
        if (explicitLockType.isIntent()) {
            lockContext.escalate(transaction);
            return;
        }

        // Case 4
        if (explicitLockType.equals(LockType.S)) {
            // S -> X
            if (parentContext != null && !LockType.canBeParentLock(parentContext.getEffectiveLockType(transaction), requestType)) {
                addIntentLock(transaction, parentContext, LockType.IX);
            }
            lockContext.promote(transaction, requestType);
        }
        else {
            // NL -> S/X
            if (parentContext != null && !LockType.canBeParentLock(parentContext.getEffectiveLockType(transaction), requestType)) {
                LockType intentLockType = requestType.equals(LockType.S) ? LockType.IS : LockType.IX;
                addIntentLock(transaction, parentContext, intentLockType);
            }
            lockContext.acquire(transaction, requestType);
        }

        return;
    }

    private static void addIntentLock(TransactionContext transaction, LockContext lockContext, LockType lockType) {
        LockContext parent = lockContext.parentContext();

        if (parent != null) {
            LockType parentLockType = parent.getEffectiveLockType(transaction);
            switch (lockType) {
                case IS:
                    if (parentLockType.equals(LockType.NL)) {
                        addIntentLock(transaction, parent, LockType.IS);
                    }
                    break;
                case IX:
                    if (parentLockType.equals(LockType.NL) || parentLockType.equals(LockType.IS)) {
                        addIntentLock(transaction, parent, LockType.IX);
                    }
                    break;
                default:
                    throw new InvalidLockException("parentContext only allow intent locks");
            }
        }

        LockType explicit = lockContext.getExplicitLockType(transaction);

        if (explicit == LockType.NL) {
            lockContext.acquire(transaction, lockType);
        } else if (explicit == LockType.IS && lockType == LockType.IX) {
            lockContext.promote(transaction, lockType);
        } else if (explicit == LockType.S && lockType == LockType.IX) {
            lockContext.promote(transaction, LockType.SIX);
        }
    }
}
