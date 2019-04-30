package edu.berkeley.cs186.database.concurrency;

public enum LockType {
    S,   // shared
    X,   // exclusive
    IS,  // intention shared
    IX,  // intention exclusive
    SIX, // shared intention exclusive
    NL;  // no lock held

    /**
     * This method checks whether lock types A and B are compatible with
     * each other. If a transaction can hold lock type A on a resource
     * at the same time another transaction holds lock type B on the same
     * resource, the lock types are compatible.
     */
    public static boolean compatible(LockType a, LockType b) {
        if (a == null || b == null) {
            throw new NullPointerException("null lock type");
        }
        //throw new UnsupportedOperationException("TODO(hw5_part1): implement");
        if (a == S) {
            if (b == IS || b == S || b == NL) return true;
            else return false;
        } else if (a == X) {
            if (b == NL) return true;
            else return false;
        } else if (a == IS) {
            if (b == X) return false;
            else return true;
        } else if (a == IX) {
            if (b == NL || b == IS || b == IX) return true;
            else return false;
        } else if (a == SIX) {
            if (b == NL || b == IS) return true;
            else return false;
        } else return true;
    }

    /**
     * This method returns the least permissive lock on the parent resource
     * that must be held for a lock of type A to be granted.
     */
    public static LockType parentLock(LockType a) {
        if (a == null) {
            throw new NullPointerException("null lock type");
        }
        //throw new UnsupportedOperationException("TODO(hw5_part1): implement");

        switch (a) {
            case S: return IS;
            case X: return IX;
            case IS: return IS;
            case IX: return IX;
            case SIX: return IX;
            case NL: return NL;
            default: throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * This method returns whether a lock can be used for a situation
     * requiring another lock (e.g. an S lock can be substituted with
     * an X lock, because an X lock allows the transaction to do everything
     * the S lock allowed it to do).
     */
    public static boolean substitutable(LockType substitute, LockType required) {
        if (required == null || substitute == null) {
            throw new NullPointerException("null lock type");
        }
        //TODO: confirm SIX substitude
        if (substitute == required) {
            return true;
        }

        if (required == S) {
            if (substitute == SIX || substitute == X) return true;
            else return false;
        } else if (required == X) {
            return false;
        } else if (required == IS) {
            if (substitute == IX || substitute == SIX) return true;
            else return false;
        } else if (required == IX) {
            if (substitute == SIX) return true;
            else return false;
        } else if (required == SIX) {
            return false;
        } else {
            return true;
        }
    }

    @Override
    public String toString() {
        switch (this) {
        case S: return "S";
        case X: return "X";
        case IS: return "IS";
        case IX: return "IX";
        case SIX: return "SIX";
        case NL: return "NL";
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }
};

