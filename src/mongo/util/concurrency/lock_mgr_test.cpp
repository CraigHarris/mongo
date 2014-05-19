// lock_mgr_test.cpp

#include "mongo/unittest/unittest.h"
#include "mongo/util/concurrency/lock_mgr.h"

namespace mongo {
    void justSleep(const TxId& blocker) { }

    TEST(LockMgrTest, SingleTx) {
        LockMgr lm;
        RecordStore* store= (RecordStore*)0x4000;
        TxId t1 = 1;
        RecordId r1 = 1;

        // acquire a shared record lock
        ASSERT( ! lm.isLocked(t1, LockMgr::SHARED_RECORD, store, r1));
        lm.acquire(t1, LockMgr::SHARED_RECORD, store, r1);
        ASSERT( lm.isLocked(t1, LockMgr::SHARED_RECORD, store, r1));

        // release a shared record lock
        lm.release(t1, LockMgr::SHARED_RECORD, store, r1);
        ASSERT( ! lm.isLocked(t1, LockMgr::SHARED_RECORD, store, r1));

        // acquire a shared record lock twice, on same RecordId
        lm.acquire(t1, LockMgr::SHARED_RECORD, store, r1);
        lm.acquire(t1, LockMgr::SHARED_RECORD, store, r1);
        ASSERT( lm.isLocked(t1, LockMgr::SHARED_RECORD, store, r1));

        // release the twice-acquired lock, with single call to release?
        lm.release(t1, LockMgr::SHARED_RECORD, store, r1);
        ASSERT( ! lm.isLocked(t1, LockMgr::SHARED_RECORD, store, r1));



        // --- test downgrade and release ---

        // acquire an exclusive then a shared lock, on the same RecordId
        lm.acquire(t1, LockMgr::EXCLUSIVE_RECORD, store, r1);
        ASSERT( lm.isLocked(t1, LockMgr::EXCLUSIVE_RECORD, store, r1));
        lm.acquire(t1, LockMgr::SHARED_RECORD, store, r1);
        ASSERT( lm.isLocked(t1, LockMgr::EXCLUSIVE_RECORD, store, r1));
        ASSERT( lm.isLocked(t1, LockMgr::SHARED_RECORD, store, r1));

        // release shared first, then exclusive
        lm.release(t1, LockMgr::SHARED_RECORD, store, r1);
        ASSERT( ! lm.isLocked(t1, LockMgr::SHARED_RECORD, store, r1));
        ASSERT( lm.isLocked(t1, LockMgr::EXCLUSIVE_RECORD, store, r1));
        lm.release(t1, LockMgr::EXCLUSIVE_RECORD, store, r1);
        ASSERT( ! lm.isLocked(t1, LockMgr::EXCLUSIVE_RECORD, store, r1));

        // release exclusive first, then shared
        lm.acquire(t1, LockMgr::EXCLUSIVE_RECORD, store, r1);
        lm.acquire(t1, LockMgr::SHARED_RECORD, store, r1);
        lm.release(t1, LockMgr::EXCLUSIVE_RECORD, store, r1);
        ASSERT( ! lm.isLocked(t1, LockMgr::EXCLUSIVE_RECORD, store, r1));
        ASSERT( lm.isLocked(t1, LockMgr::SHARED_RECORD, store, r1));
        lm.release(t1, LockMgr::SHARED_RECORD, store, r1);
        ASSERT( ! lm.isLocked(t1, LockMgr::SHARED_RECORD, store, r1));



        // --- test upgrade and release ---

        // acquire a shared, then an exclusive lock on the same RecordId
        lm.acquire(t1, LockMgr::SHARED_RECORD, store, r1);
        ASSERT( lm.isLocked(t1, LockMgr::SHARED_RECORD, store, r1));
        lm.acquire(t1, LockMgr::EXCLUSIVE_RECORD, store, r1);
        ASSERT( lm.isLocked(t1, LockMgr::SHARED_RECORD, store, r1));
        ASSERT( lm.isLocked(t1, LockMgr::EXCLUSIVE_RECORD, store, r1));

        // release exclusive first, then shared
        lm.release(t1, LockMgr::EXCLUSIVE_RECORD, store, r1);
        ASSERT( ! lm.isLocked(t1, LockMgr::EXCLUSIVE_RECORD, store, r1));
        ASSERT( lm.isLocked(t1, LockMgr::SHARED_RECORD, store, r1));
        lm.release(t1, LockMgr::SHARED_RECORD, store, r1);
        ASSERT( ! lm.isLocked(t1, LockMgr::SHARED_RECORD, store, r1));

        // release shared first, then exclusive
        lm.acquire(t1, LockMgr::SHARED_RECORD, store, r1);
        lm.acquire(t1, LockMgr::EXCLUSIVE_RECORD, store, r1);
        lm.release(t1, LockMgr::SHARED_RECORD, store, r1);
        ASSERT( ! lm.isLocked(t1, LockMgr::SHARED_RECORD, store, r1));
        ASSERT( lm.isLocked(t1, LockMgr::EXCLUSIVE_RECORD, store, r1));
        lm.release(t1, LockMgr::EXCLUSIVE_RECORD, store, r1);
        ASSERT( ! lm.isLocked(t1, LockMgr::EXCLUSIVE_RECORD, store, r1));
    }
}
