/**
*    Copyright (C) 2014 MongoDB Inc.
*
*    This program is free software: you can redistribute it and/or  modify
*    it under the terms of the GNU Affero General Public License, version 3,
*    as published by the Free Software Foundation.
*
*    This program is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*    GNU Affero General Public License for more details.
*
*    You should have received a copy of the GNU Affero General Public License
*    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*
*    As a special exception, the copyright holders give permission to link the
*    code of portions of this program with the OpenSSL library under certain
*    conditions as described in each individual source file and distribute
*    linked combinations including the program with the OpenSSL library. You
*    must comply with the GNU Affero General Public License in all respects for
*    all of the code used other than as permitted herein. If you modify file(s)
*    with this exception, you may extend this exception to your version of the
*    file(s), but you are not obligated to do so. If you do not wish to do so,
*    delete this exception statement from your version. If you delete this
*    exception statement from all source files in the program, then also delete
*    it in the license file.
*/

/**
 * tests for util/concurrency/lock_mgr.* capabilities
 *
 * Testing concurrency requires multiple threads.  In this test, these are packaged as
 * instances of a ClientTransaction class.  The test's main thread creates instances
 * of ClientTransactions and communicates with them through a pair of producer/consumer
 * buffers.  The driver thread sends requests to ClientTransaction threads using a
 * TxCommandBuffer, and waits for responses in a TxResponseBuffer. This protocol
 * allows precise control over timing.
 *
 * The producer/consumer buffer is possibly overkill. At present there is only
 * one producer and one consumer using any one buffer.  Also, the current set of
 * tests are 'lock-step', with the driver never issuing more than one command
 * before waiting for a response.
 */

#include <boost/scoped_ptr.hpp>
#include <boost/thread/thread.hpp>
#include "mongo/unittest/unittest.h"
#include "mongo/db/concurrency/lock_mgr.h"
#include "mongo/util/log.h"
#include "mongo/util/time_support.h"

namespace mongo {

    enum TxCmd {
        ACQUIRE,
        RELEASE,
        ABORT,
        POLICY,
        QUIT,
        INVALID
    };

    enum TxRsp {
        ACQUIRED,
        RELEASED,
        BLOCKED,
        AWAKENED,
        ABORTED,
        INVALID_RESPONSE
    };

    class TxResponse {
    public:
        TxRsp rspCode;
        unsigned xid;
        LockMode mode;
        ResourceId resId;
    };

    class TxRequest {
    public:
        TxCmd cmd;
        unsigned xid;
        LockMode mode;
        ResourceId resId;
        LockManager::Policy policy;
    };

    class TxResponseBuffer {
    public:
        TxResponseBuffer() : _count(0), _readPos(0), _writePos(0) { }

        void post(const TxRsp& rspCode) {
            boost::unique_lock<boost::mutex> guard(_guard);
            while (_count == 10)
                _full.wait(guard);
            buffer[_writePos++].rspCode = rspCode;
            _writePos %= 10;
            _count++;
            _empty.notify_one();
        }

        TxResponse* consume() {
            boost::unique_lock<boost::mutex> guard(_guard);
            while (_count == 0)
                _empty.wait(guard);
            TxResponse* result = &buffer[_readPos++];
            _readPos %= 10;
            _count--;
            _full.notify_one();
            return result;
        }

        boost::mutex _guard;
        boost::condition_variable _full;
        boost::condition_variable _empty;

        size_t _count;
        size_t _readPos;
        size_t _writePos;

        TxResponse buffer[10];
    };

class TxCommandBuffer {
public:
    TxCommandBuffer() : _count(0), _readPos(0), _writePos(0) { }

    void post(const TxCmd& cmd,
              const unsigned& xid = 0,
              const LockMode& mode = kShared,
              const ResourceId& resId = 0,
              const LockManager::Policy& policy = LockManager::kPolicyFirstCome) {
        boost::unique_lock<boost::mutex> guard(_guard);
        while (_count == 10)
            _full.wait(guard);
        buffer[_writePos].cmd = cmd;
        buffer[_writePos].xid = xid;
        buffer[_writePos].mode = mode;
        buffer[_writePos].resId = resId;
        buffer[_writePos].policy = policy;
        _writePos++;
        _writePos %= 10;
        _count++;
        _empty.notify_one();
    }

    TxRequest* consume() {
        boost::unique_lock<boost::mutex> guard(_guard);
        while (_count == 0)
            _empty.wait(guard);
        TxRequest* result = &buffer[_readPos++];
        _readPos %= 10;
        _count--;
        _full.notify_one();
        return result;
    }

    boost::mutex _guard;
    boost::condition_variable _full;
    boost::condition_variable _empty;

    size_t _count;
    size_t _readPos;
    size_t _writePos;

    TxRequest buffer[10];
};

class ClientTransaction : public LockManager::Notifier {
public:
    // these are called in the main driver program
    
    ClientTransaction(LockManager* lm, const unsigned& xid)
        : _lm(lm)
        , _tx(xid)
        , _thr(&ClientTransaction::processCmd, this) { }

    virtual ~ClientTransaction() { _thr.join(); }

    void acquire(const LockMode& mode, const ResourceId resId, const TxRsp& rspCode) {
        _cmd.post(ACQUIRE, _tx.getTxId(), mode, resId);
        TxResponse* rsp = _rsp.consume();
        ASSERT(rspCode == rsp->rspCode);
    }

    void release(const LockMode& mode, const ResourceId resId) {
        _cmd.post(RELEASE, _tx.getTxId(), mode, resId);
        TxResponse* rsp = _rsp.consume();
        ASSERT(RELEASED == rsp->rspCode);
    }

    void abort() {
        _cmd.post(ABORT, _tx.getTxId());
        TxResponse* rsp = _rsp.consume();
        ASSERT(ABORTED == rsp->rspCode);
    }

    void wakened() {
        TxResponse* rsp = _rsp.consume();
        ASSERT(ACQUIRED == rsp->rspCode);
    }

    void setPolicy(const LockManager::Policy& policy, const TxRsp& rspCode) {
        _cmd.post(POLICY, _tx.getTxId(), kShared, 0, policy);
        TxResponse* rsp = _rsp.consume();
        ASSERT(rspCode == rsp->rspCode);
    }

    void quit() {
        _cmd.post(QUIT);
    }
    
    // these are run within the client threads
    void processCmd() {
        bool more = true;
        while (more) {
            TxRequest* req = _cmd.consume();
            switch (req->cmd) {
            case ACQUIRE:
                try {
                    _lm->acquire(&_tx, req->mode, req->resId, this);
                    _rsp.post(ACQUIRED);
                } catch (const Transaction::AbortException& err) {
                    _tx.releaseLocks(_lm);
                    _rsp.post(ABORTED);
                    return;
                }
                break;
            case RELEASE:
                _lm->release(&_tx, req->mode, req->resId);
                _rsp.post(RELEASED);
                break;
            case ABORT:
                try {
                    _tx.abort();
                } catch (const Transaction::AbortException& err) {
                    _tx.releaseLocks(_lm);
                    _rsp.post(ABORTED);
                }
                break;
            case POLICY:
                try {
                    _lm->setPolicy(&_tx, req->policy, this);
                    _rsp.post(ACQUIRED);
                } catch( const Transaction::AbortException& err) {
                    _rsp.post(ABORTED);
                }
                break;
            case QUIT:
            default:
                more = false;
                break;
            }
        }
    }

    // inherited from Notifier, used by LockManager::acquire
    virtual void operator()(const Transaction* blocker) {
        _rsp.post(BLOCKED);
    }

    std::string toString() {
        return _tx.toString();
    }

private:
    TxCommandBuffer _cmd;
    TxResponseBuffer _rsp;
    LockManager* _lm;
    Transaction _tx;
    boost::thread _thr;
    
};

TEST(LockManagerTest, TxError) {
    useExperimentalDocLocking = true;
    LockManager lm;
    LockManager::LockStatus status;
    Transaction tx(1);
    ResourceId r1(static_cast<int>(1));
    ResourceId r2(static_cast<int>(2));

    // release a lock on a resource we haven't locked
    lm.acquire(&tx, kShared, r2);
    status = lm.release(&tx, kShared, r1); // this is in error
    ASSERT(LockManager::kLockResourceNotFound == status);
    status = lm.release(&tx, kShared, r2);
    ASSERT(LockManager::kLockReleased == status);

    // release a record we've locked in a different mode
    lm.acquire(&tx, kShared, r1);
    status = lm.release(&tx, kExclusive, r1); // this is in error
    ASSERT(LockManager::kLockModeNotFound == status);
    status = lm.release(&tx, kShared, r1);
    ASSERT(LockManager::kLockReleased == status);
}

TEST(LockManagerTest, SingleTx) {
    LockManager lm;
    Transaction t1(1);
    ResourceId r1(static_cast<int>(1));
    LockManager::LockStatus status;

    // acquire a shared record lock
    ASSERT(! lm.isLocked(&t1, kShared, r1));
    lm.acquire(&t1, kShared, r1);
    ASSERT(lm.isLocked(&t1, kShared, r1));

    // release a shared record lock
    lm.release(&t1, kShared, r1);
    ASSERT(! lm.isLocked(&t1, kShared, r1));

    // acquire a shared record lock twice, on same ResourceId
    lm.acquire(&t1, kShared, r1);
    lm.acquire(&t1, kShared, r1);
    ASSERT(lm.isLocked(&t1, kShared, r1));

    // release the twice-acquired lock, once.  Still locked
    status = lm.release(&t1, kShared, r1);
    ASSERT(LockManager::kLockCountDecremented == status);
    ASSERT(lm.isLocked(&t1, kShared, r1));

    // after 2nd release, it's not locked
    status = lm.release(&t1, kShared, r1);
    ASSERT(LockManager::kLockReleased == status);
    ASSERT(!lm.isLocked(&t1, kShared, r1));



    // --- test downgrade and release ---

    // acquire an exclusive then a shared lock, on the same ResourceId
    lm.acquire(&t1, kExclusive, r1);
    ASSERT(lm.isLocked(&t1, kExclusive, r1));
    lm.acquire(&t1, kShared, r1);
    ASSERT(lm.isLocked(&t1, kExclusive, r1));
    ASSERT(lm.isLocked(&t1, kShared, r1));

    // release shared first, then exclusive
    lm.release(&t1, kShared, r1);
    ASSERT(! lm.isLocked(&t1, kShared, r1));
    ASSERT(lm.isLocked(&t1, kExclusive, r1));
    lm.release(&t1, kExclusive, r1);
    ASSERT(! lm.isLocked(&t1, kExclusive, r1));

    // release exclusive first, then shared
    lm.acquire(&t1, kExclusive, r1);
    lm.acquire(&t1, kShared, r1);
    lm.release(&t1, kExclusive, r1);
    ASSERT(! lm.isLocked(&t1, kExclusive, r1));
    ASSERT(lm.isLocked(&t1, kShared, r1));
    lm.release(&t1, kShared, r1);
    ASSERT(! lm.isLocked(&t1, kShared, r1));



    // --- test upgrade and release ---

    // acquire a shared, then an exclusive lock on the same ResourceId
    lm.acquire(&t1, kShared, r1);
    ASSERT(lm.isLocked(&t1, kShared, r1));
    lm.acquire(&t1, kExclusive, r1);
    ASSERT(lm.isLocked(&t1, kShared, r1));
    ASSERT(lm.isLocked(&t1, kExclusive, r1));

    // release exclusive first, then shared
    lm.release(&t1, kExclusive, r1);
    ASSERT(! lm.isLocked(&t1, kExclusive, r1));
    ASSERT(lm.isLocked(&t1, kShared, r1));
    lm.release(&t1, kShared, r1);
    ASSERT(! lm.isLocked(&t1, kShared, r1));

    // release shared first, then exclusive
    lm.acquire(&t1, kShared, r1);
    lm.acquire(&t1, kExclusive, r1);
    lm.release(&t1, kShared, r1);
    ASSERT(! lm.isLocked(&t1, kShared, r1));
    ASSERT(lm.isLocked(&t1, kExclusive, r1));
    lm.release(&t1, kExclusive, r1);
    ASSERT(! lm.isLocked(&t1, kExclusive, r1));
}

TEST(LockManagerTest, TxConflict) {
    LockManager lm;

    ResourceId resLockedForIS(static_cast<int>(1));
    ResourceId resLockedForIX(static_cast<int>(2)); // locked with IX
    ResourceId resLockedForS(static_cast<int>(3)); // locked with S
    ResourceId resLockedForSIX(static_cast<int>(4)); // locked with SIX
    ResourceId resLockedForU(static_cast<int>(5)); // locked with U
    ResourceId resLockedForBX(static_cast<int>(6)); // locked with BX
    ResourceId resLockedForX(static_cast<int>(7)); // locked with X

    ClientTransaction asker(&lm, 1);
    ClientTransaction owner(&lm, 2);

    // owner acquires resources with every lock mode
    owner.acquire(kIntentShared, resLockedForIS, ACQUIRED);
    owner.acquire(kIntentExclusive, resLockedForIX, ACQUIRED);
    owner.acquire(kShared, resLockedForS, ACQUIRED);
    owner.acquire(kSharedIntentExclusive, resLockedForSIX, ACQUIRED);
    owner.acquire(kUpdate, resLockedForU, ACQUIRED);
    owner.acquire(kBlockExclusive, resLockedForBX, ACQUIRED);
    owner.acquire(kExclusive, resLockedForX, ACQUIRED);

    //
    // test request for IntentShared lock
    //
    asker.acquire(kIntentShared, resLockedForIS, ACQUIRED); // IS compatible with IS
    asker.release(kIntentShared, resLockedForIS);

    asker.acquire(kIntentShared, resLockedForIX, ACQUIRED); // IS compatible with IX
    asker.release(kIntentShared, resLockedForIX);

    asker.acquire(kIntentShared, resLockedForS, ACQUIRED); // IS compatible with S
    asker.release(kIntentShared, resLockedForS);

    asker.acquire(kIntentShared, resLockedForSIX, ACQUIRED); // IS compatible with SIX
    asker.release(kIntentShared, resLockedForSIX);

    asker.acquire(kIntentShared, resLockedForU, ACQUIRED); // IS compatible with U
    asker.release(kIntentShared, resLockedForU);

    asker.acquire(kIntentShared, resLockedForBX, ACQUIRED); // IS compatible with BX
    asker.release(kIntentShared, resLockedForBX);

    asker.acquire(kIntentShared, resLockedForX, BLOCKED); // IS conflicts with X
    owner.release(kExclusive, resLockedForX);
    asker.wakened();
    asker.release(kIntentShared, resLockedForX);
    owner.acquire(kExclusive, resLockedForX, ACQUIRED);



    //
    // test request for IntentExclusive lock
    //
    asker.acquire(kIntentExclusive, resLockedForIS, ACQUIRED); // IX compatible with IS
    asker.release(kIntentExclusive, resLockedForIS);

    asker.acquire(kIntentExclusive, resLockedForIX, ACQUIRED); // IX compatible with IX
    asker.release(kIntentExclusive, resLockedForIX);

    asker.acquire(kIntentExclusive, resLockedForS, BLOCKED); // IX conflicts with S
    owner.release(kShared, resLockedForS);
    asker.wakened();
    asker.release(kIntentExclusive, resLockedForS);
    owner.acquire(kShared, resLockedForS, ACQUIRED);

    asker.acquire(kIntentExclusive, resLockedForSIX, BLOCKED); // IX conflicts with SIX
    owner.release(kSharedIntentExclusive, resLockedForSIX);
    asker.wakened();
    asker.release(kIntentExclusive, resLockedForSIX);
    owner.acquire(kSharedIntentExclusive, resLockedForSIX, ACQUIRED);

    asker.acquire(kIntentExclusive, resLockedForU, BLOCKED); // IX conflicts with U
    owner.release(kUpdate, resLockedForU);
    asker.wakened();
    asker.release(kIntentExclusive, resLockedForU);
    owner.acquire(kUpdate, resLockedForU, ACQUIRED);

    asker.acquire(kIntentExclusive, resLockedForBX, BLOCKED); // IX conflicts with BX
    owner.release(kBlockExclusive, resLockedForBX);
    asker.wakened();
    asker.release(kIntentExclusive, resLockedForBX);
    owner.acquire(kBlockExclusive, resLockedForBX, ACQUIRED);

    asker.acquire(kIntentExclusive, resLockedForX, BLOCKED); // IX conflicts with X
    owner.release(kExclusive, resLockedForX);
    asker.wakened();
    asker.release(kIntentExclusive, resLockedForX);
    owner.acquire(kExclusive, resLockedForX, ACQUIRED);



    //
    // test request for Shared lock
    //
    asker.acquire(kShared, resLockedForIS, ACQUIRED); // S compatible with IS
    asker.release(kShared, resLockedForIS);

    asker.acquire(kShared, resLockedForIX, BLOCKED); // S conflicts with IX
    owner.release(kIntentExclusive, resLockedForIX);
    asker.wakened();
    asker.release(kShared, resLockedForIX);
    owner.acquire(kIntentExclusive, resLockedForIX, ACQUIRED);

    asker.acquire(kShared, resLockedForS, ACQUIRED); // S compatible with S
    asker.release(kShared, resLockedForS);

    asker.acquire(kShared, resLockedForSIX, BLOCKED); // S conflicts with SIX
    owner.release(kSharedIntentExclusive, resLockedForSIX);
    asker.wakened();
    asker.release(kShared, resLockedForSIX);
    owner.acquire(kSharedIntentExclusive, resLockedForSIX, ACQUIRED);

    asker.acquire(kShared, resLockedForU, ACQUIRED); // S compatible with U
    asker.release(kShared, resLockedForU);

    asker.acquire(kShared, resLockedForBX, ACQUIRED); // S compatible with BX
    asker.release(kShared, resLockedForBX);

    asker.acquire(kShared, resLockedForX, BLOCKED); // S conflicts with X
    owner.release(kExclusive, resLockedForX);
    asker.wakened();
    asker.release(kShared, resLockedForX);
    owner.acquire(kExclusive, resLockedForX, ACQUIRED);



    //
    // test request for SharedIntentExclusive lock
    //
    asker.acquire(kSharedIntentExclusive, resLockedForIS, ACQUIRED); // SIX compatible with IS
    asker.release(kSharedIntentExclusive, resLockedForIS);

    asker.acquire(kSharedIntentExclusive, resLockedForIX, BLOCKED); // SIX conflicts with IX
    owner.release(kIntentExclusive, resLockedForIX);
    asker.wakened();
    asker.release(kSharedIntentExclusive, resLockedForIX);
    owner.acquire(kIntentExclusive, resLockedForIX, ACQUIRED);

    asker.acquire(kSharedIntentExclusive, resLockedForS, BLOCKED); // SIX conflicts with S
    owner.release(kShared, resLockedForS);
    asker.wakened();
    asker.release(kSharedIntentExclusive, resLockedForS);
    owner.acquire(kShared, resLockedForS, ACQUIRED);

    asker.acquire(kSharedIntentExclusive, resLockedForSIX, BLOCKED); // SIX conflicts with SIX
    owner.release(kSharedIntentExclusive, resLockedForSIX);
    asker.wakened();
    asker.release(kSharedIntentExclusive, resLockedForSIX);
    owner.acquire(kSharedIntentExclusive, resLockedForSIX, ACQUIRED);

    asker.acquire(kSharedIntentExclusive, resLockedForU, BLOCKED); // SIX conflicts with U
    owner.release(kUpdate, resLockedForU);
    asker.wakened();
    asker.release(kSharedIntentExclusive, resLockedForU);
    owner.acquire(kUpdate, resLockedForU, ACQUIRED);

    asker.acquire(kSharedIntentExclusive, resLockedForBX, BLOCKED); // SIX conflicts with BX
    owner.release(kBlockExclusive, resLockedForBX);
    asker.wakened();
    asker.release(kSharedIntentExclusive, resLockedForBX);
    owner.acquire(kBlockExclusive, resLockedForBX, ACQUIRED);

    asker.acquire(kSharedIntentExclusive, resLockedForX, BLOCKED); // SIX conflicts with X
    owner.release(kExclusive, resLockedForX);
    asker.wakened();
    asker.release(kSharedIntentExclusive, resLockedForX);
    owner.acquire(kExclusive, resLockedForX, ACQUIRED);



    //
    // test request for Update lock
    //
    asker.acquire(kUpdate, resLockedForIS, ACQUIRED); // U compatible with IS
    asker.release(kUpdate, resLockedForIS);

    asker.acquire(kUpdate, resLockedForIX, BLOCKED); // U conflicts with IX
    owner.release(kIntentExclusive, resLockedForIX);
    asker.wakened();
    asker.release(kUpdate, resLockedForIX);
    owner.acquire(kIntentExclusive, resLockedForIX, ACQUIRED);

    asker.acquire(kUpdate, resLockedForS, ACQUIRED); // U compatible with S
    asker.release(kUpdate, resLockedForS);

    asker.acquire(kUpdate, resLockedForSIX, BLOCKED); // U conflicts with SIX
    owner.release(kSharedIntentExclusive, resLockedForSIX);
    asker.wakened();
    asker.release(kUpdate, resLockedForSIX);
    owner.acquire(kSharedIntentExclusive, resLockedForSIX, ACQUIRED);

    asker.acquire(kUpdate, resLockedForU, BLOCKED); // U conflicts with U
    owner.release(kUpdate, resLockedForU);
    asker.wakened();
    asker.release(kUpdate, resLockedForU);
    owner.acquire(kUpdate, resLockedForU, ACQUIRED);

    asker.acquire(kUpdate, resLockedForBX, BLOCKED); // U conflicts with BX
    owner.release(kBlockExclusive, resLockedForBX);
    asker.wakened();
    asker.release(kUpdate, resLockedForBX);
    owner.acquire(kBlockExclusive, resLockedForBX, ACQUIRED);

    asker.acquire(kUpdate, resLockedForX, BLOCKED); // U conflicts with X
    owner.release(kExclusive, resLockedForX);
    asker.wakened();
    asker.release(kUpdate, resLockedForX);
    owner.acquire(kExclusive, resLockedForX, ACQUIRED);



    //
    // test request for BlockExclusive lock
    //
    asker.acquire(kBlockExclusive, resLockedForIS, ACQUIRED); // BX compatible with IS
    asker.release(kBlockExclusive, resLockedForIS);

    asker.acquire(kBlockExclusive, resLockedForIX, BLOCKED); // BX conflicts with IX
    owner.release(kIntentExclusive, resLockedForIX);
    asker.wakened();
    asker.release(kBlockExclusive, resLockedForIX);
    owner.acquire(kIntentExclusive, resLockedForIX, ACQUIRED);

    asker.acquire(kBlockExclusive, resLockedForS, ACQUIRED); // BX compatible with S
    asker.release(kBlockExclusive, resLockedForS);

    asker.acquire(kBlockExclusive, resLockedForSIX, BLOCKED); // BX conflicts with SIX
    owner.release(kSharedIntentExclusive, resLockedForSIX);
    asker.wakened();
    asker.release(kBlockExclusive, resLockedForSIX);
    owner.acquire(kSharedIntentExclusive, resLockedForSIX, ACQUIRED);

    asker.acquire(kBlockExclusive, resLockedForU, BLOCKED); // BX conflicts with U
    owner.release(kUpdate, resLockedForU);
    asker.wakened();
    asker.release(kBlockExclusive, resLockedForU);
    owner.acquire(kUpdate, resLockedForU, ACQUIRED);

    asker.acquire(kBlockExclusive, resLockedForBX, ACQUIRED); // BX compatible with BX
    asker.release(kBlockExclusive, resLockedForBX);

    asker.acquire(kBlockExclusive, resLockedForX, BLOCKED); // BX conflicts with X
    owner.release(kExclusive, resLockedForX);
    asker.wakened();
    asker.release(kBlockExclusive, resLockedForX);
    owner.acquire(kExclusive, resLockedForX, ACQUIRED);



    //
    // test request for Exclusive lock
    //
    asker.acquire(kExclusive, resLockedForIS, BLOCKED);
    owner.release(kIntentShared, resLockedForIS);
    asker.wakened();
    asker.release(kExclusive, resLockedForIS);

    asker.acquire(kExclusive, resLockedForIX, BLOCKED);
    owner.release(kIntentExclusive, resLockedForIX);
    asker.wakened();
    asker.release(kExclusive, resLockedForIX);

    asker.acquire(kExclusive, resLockedForS, BLOCKED);
    owner.release(kShared, resLockedForS);
    asker.wakened();
    asker.release(kExclusive, resLockedForS);

    asker.acquire(kExclusive, resLockedForSIX, BLOCKED);
    owner.release(kSharedIntentExclusive, resLockedForSIX);
    asker.wakened();
    asker.release(kExclusive, resLockedForSIX);

    asker.acquire(kExclusive, resLockedForU, BLOCKED);
    owner.release(kUpdate, resLockedForU);
    asker.wakened();
    asker.release(kExclusive, resLockedForU);

    asker.acquire(kExclusive, resLockedForBX, BLOCKED);
    owner.release(kBlockExclusive, resLockedForBX);
    asker.wakened();
    asker.release(kExclusive, resLockedForBX);

    asker.acquire(kExclusive, resLockedForX, BLOCKED);
    owner.release(kExclusive, resLockedForX);
    asker.wakened();
    asker.release(kExclusive, resLockedForX);

    asker.quit();
    owner.quit();
}

TEST(LockManagerTest, TxSimpleDeadlocks) {
    LockManager lm(LockManager::kPolicyFirstCome);
    ClientTransaction t1(&lm, 1);

    ClientTransaction a1(&lm, 2);
    ClientTransaction a2(&lm, 3);
    ClientTransaction a3(&lm, 4);
    ClientTransaction a4(&lm, 5);
    ClientTransaction a5(&lm, 6);
    ClientTransaction a6(&lm, 7);

    ResourceId r1(static_cast<int>(1));
    ResourceId r2(static_cast<int>(2));

    // two transactions:
    //     one reads resource 1 and writes resource 2
    //     two reads resource 2 and writes resource 1

    // simple deadlock test 1: both read first, then write, write deadlocks
    a1.acquire(kShared, r2, ACQUIRED);
    t1.acquire(kShared, r1, ACQUIRED);
    t1.acquire(kExclusive, r2, BLOCKED);
    a1.acquire(kExclusive, r1, ABORTED);
    t1.wakened();
    t1.release(kExclusive, r2);
    t1.release(kShared, r1);

    // simple deadlock test 2: both write first, then read, read deadlocks
    a2.acquire(kExclusive, r2, ACQUIRED);
    t1.acquire(kExclusive, r1, ACQUIRED);
    t1.acquire(kShared, r2, BLOCKED);
    a2.acquire(kShared, r1, ABORTED);
    t1.wakened();
    t1.release(kShared, r2);
    t1.release(kExclusive, r1);


    //
    // two transactions, one writes two resource, the other reads/writes
    //

    // simple deadlock test 3: one reads first, other writes first, read deadlocks
    a3.acquire(kExclusive, r2, ACQUIRED);
    t1.acquire(kShared, r1, ACQUIRED);
    t1.acquire(kExclusive, r2, BLOCKED);
    a3.acquire(kExclusive, r1, ABORTED);
    t1.wakened();
    t1.release(kExclusive, r2);
    t1.release(kShared, r1);

    //
    // two transactions: one reads and the other writes the same resources
    //

    // simple deadlock test 4: reader deadlocks
    a4.acquire(kShared, r2, ACQUIRED);
    t1.acquire(kExclusive, r1, ACQUIRED);
    t1.acquire(kExclusive, r2, BLOCKED);
    a4.acquire(kShared, r1, ABORTED);
    t1.wakened();
    t1.release(kExclusive, r2);
    t1.release(kExclusive, r1);

    // simple deadlock test 5: writer deadlocks
    a5.acquire(kExclusive, r2, ACQUIRED);
    t1.acquire(kShared, r1, ACQUIRED);
    t1.acquire(kShared, r2, BLOCKED);
    a5.acquire(kExclusive, r1, ABORTED);
    t1.wakened();
    t1.release(kShared, r2);
    t1.release(kShared, r1);

    //
    // two transactions: both write the same resources
    //

    // simple deadlock test 6: reader deadlocks
    a6.acquire(kExclusive, r2, ACQUIRED);
    t1.acquire(kExclusive, r1, ACQUIRED);
    t1.acquire(kExclusive, r2, BLOCKED);
    a6.acquire(kExclusive, r1, ABORTED);
    t1.wakened();
    t1.release(kExclusive, r2);
    t1.release(kExclusive, r1);

    t1.quit();
}

TEST(LockManagerTest, TxComplexDeadlocks) {
    LockManager lm(LockManager::kPolicyFirstCome);
    ClientTransaction t1(&lm, 1);
    ClientTransaction t2(&lm, 2);

    ClientTransaction a1(&lm, 3);
    ClientTransaction a2(&lm, 4);

    ResourceId r1(static_cast<int>(1));
    ResourceId r2(static_cast<int>(2));
    ResourceId r3(static_cast<int>(3));

    // three way deadlock: each read and write the same resources
    t1.acquire(kShared, r1, ACQUIRED);
    t2.acquire(kShared, r2, ACQUIRED);
    a1.acquire(kShared, r3, ACQUIRED);
    t1.acquire(kExclusive, r2, BLOCKED);
    t2.acquire(kExclusive, r3, BLOCKED);
    a1.acquire(kExclusive, r1, ABORTED);;
    t2.wakened(); // with a1's lock release, t2 should wake
    t2.release(kShared, r2);
    t1.wakened(); // with t2's locks released, t1 should wake
    t2.release(kExclusive, r3);
    t1.release(kShared, r1);
    t1.release(kExclusive, r2);

    // test for phantom deadlocks
    t1.acquire(kShared, r1, ACQUIRED);
    t2.acquire(kExclusive, r1, BLOCKED);
    t1.release(kShared, r1);
    t2.wakened();
    // at this point, t2 should no longer be waiting for t1
    // so it should be OK for t1 to wait for t2
    t1.acquire(kShared, r1, BLOCKED);
    t2.release(kExclusive, r1);
    t1.wakened();
    t1.release(kShared, r1);

    // test for missing deadlocks: due to downgrades
    a2.acquire(kExclusive, r1, ACQUIRED);
    a2.acquire(kShared, r1, ACQUIRED);
    t2.acquire(kShared, r2, ACQUIRED); // setup for deadlock with a2
    t2.acquire(kExclusive, r1, BLOCKED); // block on a2
    a2.release(kExclusive, r1);
    // at this point, t2 should still be blocked on a2's downgraded lock
    // So a2 should not be allowed to wait on t2's resource.
    a2.acquire(kExclusive, r2, ABORTED);
    t2.wakened();
    t2.release(kShared, r2);
    t2.release(kExclusive, r1);

    t1.quit();
    t2.quit();
}

TEST(LockManagerTest, TxDowngrade) {
    LockManager lm;
    ClientTransaction t1(&lm, 1);
    ClientTransaction t2(&lm, 2);
    ResourceId r1(static_cast<int>(1));

    t1.acquire(kExclusive, r1, ACQUIRED);
    t1.acquire(kShared, r1, ACQUIRED); // downgrade
    // t1 still has exclusive on resource 1, so t2 must wait
    t2.acquire(kShared, r1, BLOCKED);
    t1.release(kExclusive, r1);
    t2.wakened(); // with the exclusive lock released, t2 wakes
    t1.release(kShared, r1);
    t2.release(kShared, r1);

    t1.acquire(kExclusive, r1, ACQUIRED);
    t1.acquire(kShared, r1, ACQUIRED); // downgrade
    // t1 still has exclusive on resource 1, so t2 must wait
    t2.acquire(kShared, r1, BLOCKED);
    t1.release(kShared, r1);
    // with t1 still holding exclusive on resource 1, t2 still blocked
    t1.release(kExclusive, r1);
    t2.wakened(); // with the exclusive lock released, t2 wakes
    t2.release(kShared, r1);

    t1.acquire(kExclusive, r1, ACQUIRED);
    // t1 has exclusive on resource 1, so t2 must wait
    t2.acquire(kShared, r1, BLOCKED);
    // even though t2 is waiting for resource 1, t1 can still use it shared,
    // because it already owns exclusive lock and can't block on itself
    t1.acquire(kShared, r1, ACQUIRED);
    t1.release(kExclusive, r1);
    t2.wakened(); // with the exclusive lock released, t2 wakes
    t1.release(kShared, r1);
    t2.release(kShared, r1);

    // t2 acquires exclusive during t1's downgrade
    t1.acquire(kExclusive, r1, ACQUIRED);
    t1.acquire(kShared, r1, ACQUIRED);
    t2.acquire(kExclusive, r1, BLOCKED);
    t1.release(kExclusive, r1);
    t1.release(kShared, r1);
    t2.wakened();
    t2.release(kExclusive, r1);

    t1.acquire(kExclusive, r1, ACQUIRED);
    t2.acquire(kExclusive, r1, BLOCKED);
    t1.acquire(kShared, r1, ACQUIRED);
    t1.release(kExclusive, r1);
    t1.release(kShared, r1);
    t2.wakened();
    t2.release(kExclusive, r1);

    t1.quit();
    t2.quit();
}

TEST(LockManagerTest, TxUpgrade) {
    LockManager lm(LockManager::kPolicyOldestTxFirst);
    ClientTransaction t1(&lm, 3);
    ClientTransaction t2(&lm, 4);
    ClientTransaction t3(&lm, 5);

    ClientTransaction a2(&lm, 1);
    ClientTransaction a3(&lm, 2);

    ResourceId r1(static_cast<int>(1));

    // test upgrade succeeds, blocks subsequent reads
    t1.acquire(kShared, r1, ACQUIRED);
    t1.acquire(kExclusive, r1, ACQUIRED); // upgrade
    t2.acquire(kShared, r1, BLOCKED);
    t1.release(kExclusive, r1);
    t2.wakened();
    t1.release(kShared, r1);
    t2.release(kShared, r1);

    // test upgrade blocks, then wakes
    t1.acquire(kShared, r1, ACQUIRED);
    t2.acquire(kShared, r1, ACQUIRED);
    // t1 can't use resource 1 exclusively yet, because t2 is using it
    t1.acquire(kExclusive, r1, BLOCKED);
    t2.release(kShared, r1);
    t1.wakened(); // with t2's shared lock released, t1 wakes
    t1.release(kExclusive, r1);
    t1.release(kShared, r1);

    // test upgrade blocks on several, then wakes
    t2.acquire(kShared, r1, ACQUIRED);
    t3.acquire(kShared, r1, ACQUIRED);
    // t2 can't use resource 1 exclusively yet, because t3 is using it
    t2.acquire(kExclusive, r1, BLOCKED);
    t1.acquire(kShared, r1, ACQUIRED); // additional blocker
    t3.release(kShared, r1); // t2 still blocked
    t1.release(kShared, r1);
    t2.wakened(); // with t1's shared lock released, t2 wakes
    t2.release(kExclusive, r1);
    t2.release(kShared, r1);

    // failure to upgrade
    t1.acquire(kShared, r1, ACQUIRED);
    a2.acquire(kShared, r1, ACQUIRED);
    t1.acquire(kExclusive, r1, BLOCKED);
    a2.acquire(kExclusive, r1, ABORTED);
    // with a2's abort, t1 can wake
    t1.wakened();
    t1.release(kShared, r1);
    t1.release(kExclusive, r1);

    // failure to upgrade
    t1.acquire(kShared, r1, ACQUIRED);
    t2.acquire(kShared, r1, ACQUIRED);
    t1.acquire(kExclusive, r1, BLOCKED);
    a3.acquire(kShared, r1, ACQUIRED);
    t2.release(kShared, r1); // t1 still blocked on a3
    a3.acquire(kExclusive, r1, ABORTED);
    t1.wakened();
    t1.release(kExclusive, r1);
    t1.release(kShared, r1);

    t1.quit();
    t2.quit();
    t3.quit();
}

TEST(LockManagerTest, TxPolicy) {
    ResourceId r1(static_cast<int>(1));
    ResourceId r2(static_cast<int>(2));

    {
        // Test FirstComeFirstServe policy
        LockManager lm_first;
        ClientTransaction t1(&lm_first, 1);
        ClientTransaction t2(&lm_first, 2);
        ClientTransaction t3(&lm_first, 3);
        // test1
        t1.acquire(kExclusive, r1, ACQUIRED);
        t2.acquire(kShared, r1, BLOCKED);
        t3.acquire(kExclusive, r1, BLOCKED);
        t1.release(kExclusive, r1);
        // t2 should wake first, because its request came before t3's
        t2.wakened();
        t2.release(kShared, r1);
        t3.wakened();
        t3.release(kExclusive, r1);

        // test2
        t1.acquire(kExclusive, r1, ACQUIRED);
        t3.acquire(kExclusive, r1, BLOCKED);
        t2.acquire(kShared, r1, BLOCKED);
        t1.release(kExclusive, r1);
        // t3 should wake first, because its request came before t2's
        t3.wakened();
        t3.release(kExclusive, r1);
        t2.wakened();
        t2.release(kShared, r1);

        t1.quit();
        t2.quit();
        t3.quit();
    }

    {
        // Test OLDEST_TX_FIRST policy
        // for now, smaller TxIds are considered older

        LockManager lm_oldest(LockManager::kPolicyOldestTxFirst);
        ClientTransaction t1(&lm_oldest, 1);
        ClientTransaction t2(&lm_oldest, 2);
        ClientTransaction t3(&lm_oldest, 3);

        // test 1
        t1.acquire(kExclusive, r1, ACQUIRED);
        t3.acquire(kExclusive, r1, BLOCKED);
        t2.acquire(kShared, r1, BLOCKED);
        t1.release(kExclusive, r1);

        // t2 should wake first, even though t3 came first in time
        // because t2 is older than t3
        t2.wakened();
        t2.release(kShared, r1);
        t3.wakened();
        t3.release(kExclusive, r1);

        // test 2
        t1.acquire(kExclusive, r1, ACQUIRED);
        t2.acquire(kShared, r1, BLOCKED);
        t3.acquire(kExclusive, r1, BLOCKED);
        t1.release(kExclusive, r1);

        // t2 should wake first, because it's older than t3
        t2.wakened();
        t2.release(kShared, r1);
        t3.wakened();
        t3.release(kExclusive, r1);

        t1.quit();
        t2.quit();
        t3.quit();
    }

    {
        LockManager lm_blockers(LockManager::kPolicyBlockersFirst);
        ClientTransaction t1(&lm_blockers, 1);
        ClientTransaction t2(&lm_blockers, 2);
        ClientTransaction t3(&lm_blockers, 3);
        ClientTransaction t4(&lm_blockers, 4);

        // BIGGEST_BLOCKER_FIRST policy

        // set up t3 as the biggest blocker
        t3.acquire(kExclusive, r2, ACQUIRED);
        t4.acquire(kExclusive, r2, BLOCKED);

        // test 1
        t1.acquire(kExclusive, r1, ACQUIRED);
        t3.acquire(kExclusive, r1, BLOCKED);
        t2.acquire(kShared, r1, BLOCKED);
        t1.release(kExclusive, r1);
        // t3 should wake first, because it's a bigger blocker than t2
        t3.wakened();
        t3.release(kExclusive, r1);
        t2.wakened();
        t2.release(kShared, r1);

        // test 2
        t1.acquire(kExclusive, r1, ACQUIRED);
        t2.acquire(kShared, r1, BLOCKED);
        t3.acquire(kExclusive, r1, BLOCKED);
        t1.release(kExclusive, r1);
        // t3 should wake first, even though t2 came first,
        // because it's a bigger blocker than t2
        t3.wakened();
        t3.release(kExclusive, r1);
        t2.wakened();
        t2.release(kShared, r1);

        t3.release(kExclusive, r2);
        t4.wakened();
        t4.release(kExclusive, r2);

        t1.quit();
        t2.quit();
        t3.quit();
        t4.quit();
    }
}

/*
 * test kPolicyReadersOnly and kPolicyWritersOnly
 */
TEST(LockManagerTest, TxOnlyPolicies) {
    LockManager lm;
    ClientTransaction t1(&lm, 1);
    ClientTransaction t2(&lm, 2);
    ClientTransaction t3(&lm, 3);
    ClientTransaction t4(&lm, 4);
    ClientTransaction t5(&lm, 5);
    ClientTransaction tp(&lm, 6);
    ResourceId r1(static_cast<int>(1));
    ResourceId r2(static_cast<int>(2));

    // show kPolicyReadersOnly blocking writers, which
    // awake when policy reverts
    t1.acquire(kShared, r1, ACQUIRED);
    tp.setPolicy(LockManager::kPolicyReadersOnly, ACQUIRED);
    t3.acquire(kExclusive, r2, BLOCKED); // just policy conflict
    t4.acquire(kExclusive, r1, BLOCKED); // both policy & t1
    t5.acquire(kShared, r1, ACQUIRED);   // even tho t4
    tp.setPolicy(LockManager::kPolicyFirstCome, ACQUIRED);
    t3.wakened();
    t3.release(kExclusive, r2);
    t1.release(kShared, r1);
    t5.release(kShared, r1);
    t4.wakened();
    t4.release(kExclusive, r1);

    // show WRITERS_ONLY blocking readers, which
    // awake when policy reverts
    t1.acquire(kExclusive, r1, ACQUIRED);
    tp.setPolicy(LockManager::kPolicyWritersOnly, ACQUIRED);
    t3.acquire(kShared, r2, BLOCKED);       // just policy conflict
    t4.acquire(kShared, r1, BLOCKED);       // both policy & t1
    t1.release(kExclusive, r1);
    t5.acquire(kExclusive, r2, ACQUIRED);   // even tho t3
    t5.release(kExclusive, r2);
    tp.setPolicy(LockManager::kPolicyFirstCome, ACQUIRED);
    t3.wakened();
    t3.release(kShared, r2);
    t4.wakened();
    t4.release(kShared, r1);

    // show READERS_ONLY blocked by existing writer
    // but still blocking new writers
    t1.acquire(kExclusive, r1, ACQUIRED);
    tp.setPolicy(LockManager::kPolicyFirstCome, BLOCKED);  // blocked by t1
    t2.acquire(kExclusive, r2, BLOCKED);   // just policy conflict
    t3.acquire(kShared, r2, ACQUIRED);     // even tho t2
    t3.release(kShared, r2);
    t1.release(kExclusive, r1);
    tp.wakened();
    tp.setPolicy(LockManager::kPolicyFirstCome, ACQUIRED);
    t2.wakened();
    t2.release(kExclusive, r2);

    // show WRITERS_ONLY blocked by existing reader
    // but still blocking new readers
    t1.acquire(kShared, r1, ACQUIRED);
    tp.setPolicy(LockManager::kPolicyWritersOnly, BLOCKED);  // blocked by t1
    t2.acquire(kShared, r2, BLOCKED);      // just policy conflict
    t1.release(kShared, r1);
    tp.wakened();
    tp.setPolicy(LockManager::kPolicyFirstCome, ACQUIRED);
    t2.wakened();
    t2.release(kShared, r2);

    t1.quit();
    t2.quit();
    t3.quit();
    t4.quit();
    t5.quit();
    tp.quit();
}

TEST(LockManagerTest, TxShutdown) {
    LockManager lm;
    ClientTransaction t1(&lm, 1);
    ClientTransaction t2(&lm, 2);
    ResourceId r1(static_cast<int>(1));
    ResourceId r2(static_cast<int>(2));
    ResourceId r3(static_cast<int>(3));
    ResourceId r4(static_cast<int>(4));

    t1.acquire(kShared, r1, ACQUIRED);
    lm.shutdown(3000);

    // t1 can still do work while quiescing
    t1.release(kShared, r1);
    t1.acquire(kShared, r2, ACQUIRED);
#ifdef TRANSACTION_REGISTRATION
    // t2 is new and should be refused
    t2.acquire(kShared, r3, ABORTED);
#else
    t2.quit();
#endif
    // after the quiescing period, t1's request should be refused
    sleepsecs(3);
    t1.acquire(kShared, r4, ABORTED);
}
}
