/**
*    Copyright (C) MongoDB Inc.
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

#include <boost/thread/thread.hpp>
#include "mongo/unittest/unittest.h"
#include "mongo/util/concurrency/lock_mgr.h"
#include "mongo/util/log.h"

namespace mongo {

    enum TxCmd {
	ACQUIRE,
	RELEASE,
	ABORT,
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
	TxId xid;
	LockMgr::LockMode mode;
	const RecordStore* store;
	RecordId recId;
    };

    class TxRequest {
    public:
	TxCmd cmd;
	TxId xid;
	LockMgr::LockMode mode;
	const RecordStore* store;
	RecordId recId;
    };

    class TxResponseBuffer {
    public:
	TxResponseBuffer() : _count(0), _readPos(0), _writePos(0) { }

	void post( const TxRsp& rspCode) {
	    boost::unique_lock<boost::mutex> guard(_guard);
	    while (_count == 10)
		_full.wait(guard);
	    buffer[_writePos++].rspCode = rspCode;
	    _writePos %= 10;
	    _count++;
	    _empty.notify_one( );
	}

	TxResponse* consume( ) {
	    boost::unique_lock<boost::mutex> guard(_guard);
	    while (_count == 0)
		_empty.wait(guard);
	    TxResponse* result = &buffer[_readPos++];
	    _readPos %= 10;
	    _count--;
	    _full.notify_one( );
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

	void post( const TxCmd& cmd,
		   const TxId& xid,
		   const LockMgr::LockMode& mode,
		   const RecordStore* store,
		   const RecordId& recId
	    ) {
	    boost::unique_lock<boost::mutex> guard(_guard);
	    while (_count == 10)
		_full.wait(guard);
	    buffer[_writePos].cmd = cmd;
	    buffer[_writePos].xid = xid;
	    buffer[_writePos].mode = mode;
	    buffer[_writePos].store = store;
	    buffer[_writePos].recId = recId;
            _writePos++;
	    _writePos %= 10;
	    _count++;
	    _empty.notify_one( );
	}

	TxRequest* consume( ) {
	    boost::unique_lock<boost::mutex> guard(_guard);
	    while (_count == 0)
		_empty.wait(guard);
	    TxRequest* result = &buffer[_readPos++];
	    _readPos %= 10;
	    _count--;
	    _full.notify_one( );
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

    class ClientTransaction : public LockMgr::Notifier {
    public:
	// these are called in the main driver program
	
	ClientTransaction( LockMgr* lm, const TxId& xid) : _lm(lm), _xid(xid), _thr(&ClientTransaction::processCmd, this) { }
	virtual ~ClientTransaction( ) { _thr.join(); }

	void acquire( const LockMgr::LockMode& mode, const RecordId recId, const TxRsp& rspCode ) {
	    _cmd.post( ACQUIRE, _xid, mode, (RecordStore*)0x4000, recId );
	    TxResponse* rsp = _rsp.consume( );
	    ASSERT( rspCode == rsp->rspCode );
	}

	void release( const LockMgr::LockMode& mode, const RecordId recId ) {
	    _cmd.post( RELEASE, _xid, mode, (RecordStore*)0x4000, recId );
	    TxResponse* rsp = _rsp.consume( );
	    ASSERT( RELEASED == rsp->rspCode );
	}

	void abort( ) {
	    _cmd.post( ABORT, _xid, LockMgr::INVALID, (RecordStore*)0, 0 );
	    TxResponse* rsp = _rsp.consume( );
	    ASSERT( ABORTED == rsp->rspCode );
	}

	void wakened( ) {
	    TxResponse* rsp = _rsp.consume( );
	    ASSERT( ACQUIRED == rsp->rspCode );
	}

	void quit( ) {
	    _cmd.post( QUIT, 0, LockMgr::INVALID, 0, 0 );
	}
	
	// these are run within the client threads
	void processCmd( ) {
	    bool more = true;
	    while (more) {
		TxRequest* req = _cmd.consume( );
		switch (req->cmd) {
		case ACQUIRE:
                    try {
                        _lm->acquire(_xid, req->mode, req->store, req->recId, this);
                        _rsp.post(ACQUIRED);
                    } catch (const AbortException& err) {
                        _rsp.post(ABORTED);
                    }
		    break;
		case RELEASE:
		    _lm->release(_xid, req->mode, req->store, req->recId);
		    _rsp.post(RELEASED);
		    break;
		case ABORT:
		    _lm->abort(_xid);
		    _rsp.post(ABORTED);
		    break;
		case QUIT:
//                  log() << "t" << _xid << ": in QUIT" << endl;
		default:
                    more = false;
		    break;
		}
	    }
            log() << "t" << _xid << ": ending" << endl;
	}

	// inherited from Notifier, used by LockMgr::acquire
	virtual void operator()(const TxId& blocker) {
//	    log() << "in sleep notifier" << endl;
	    _rsp.post(BLOCKED);
	}

    private:
	TxCommandBuffer _cmd;
	TxResponseBuffer _rsp;
	LockMgr* _lm;
	TxId _xid;
	boost::thread _thr;
	
    };

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

    TEST(LockMgrTest, SimpleTwoTx) {
	LockMgr lm;
	ClientTransaction t1( &lm, 1 );
	ClientTransaction t2( &lm, 2 );

        try {

            // no conflicts with shared locks on same/different objects

            t1.acquire( LockMgr::SHARED_RECORD, 1, ACQUIRED );
            t2.acquire( LockMgr::SHARED_RECORD, 2, ACQUIRED );
            t1.acquire( LockMgr::SHARED_RECORD, 2, ACQUIRED );
            t2.acquire( LockMgr::SHARED_RECORD, 1, ACQUIRED );

            t1.release( LockMgr::SHARED_RECORD, 1 );
            t1.release( LockMgr::SHARED_RECORD, 2 );
            t2.release( LockMgr::SHARED_RECORD, 1 );
            t2.release( LockMgr::SHARED_RECORD, 2 );


	    // no conflicts with exclusive locks on different objects
	    t1.acquire( LockMgr::EXCLUSIVE_RECORD, 1, ACQUIRED );
	    t2.acquire( LockMgr::EXCLUSIVE_RECORD, 2, ACQUIRED );
	    t1.release( LockMgr::EXCLUSIVE_RECORD, 1);
	    t2.release( LockMgr::EXCLUSIVE_RECORD, 2);


            // simple lock conflicts

            t1.acquire( LockMgr::SHARED_RECORD, 1, ACQUIRED );
	    // t2's request is incompatible with t1's lock, so it should block
            t2.acquire( LockMgr::EXCLUSIVE_RECORD, 1, BLOCKED );
            t1.release( LockMgr::SHARED_RECORD, 1 );
            t2.wakened( ); // with t1's lock released, t2 should wake
            t2.release( LockMgr::EXCLUSIVE_RECORD, 1 );

            t1.acquire( LockMgr::EXCLUSIVE_RECORD, 1, ACQUIRED );
            t2.acquire( LockMgr::SHARED_RECORD, 1, BLOCKED );
            t1.release( LockMgr::EXCLUSIVE_RECORD, 1 );
            t2.wakened( );
            t2.release( LockMgr::SHARED_RECORD, 1 );

            t1.acquire( LockMgr::EXCLUSIVE_RECORD, 1, ACQUIRED );
            t2.acquire( LockMgr::EXCLUSIVE_RECORD, 1, BLOCKED );
            t1.release( LockMgr::EXCLUSIVE_RECORD, 1);
            t2.wakened( );
            t2.release( LockMgr::EXCLUSIVE_RECORD, 1);


            // deadlock test 1

            t1.acquire( LockMgr::SHARED_RECORD, 1, ACQUIRED );
            t2.acquire( LockMgr::SHARED_RECORD, 2, ACQUIRED );
            t1.acquire( LockMgr::EXCLUSIVE_RECORD, 2, BLOCKED );
	    // t2's request would form a dependency cycle, so it should abort
            t2.acquire( LockMgr::EXCLUSIVE_RECORD, 1, ABORTED );
            t1.wakened( ); // with t2's locks released, t1 should wake
            t1.release( LockMgr::EXCLUSIVE_RECORD, 2);
            t1.release( LockMgr::SHARED_RECORD, 1);

	    // deadlock test 2

            t1.acquire( LockMgr::SHARED_RECORD, 1, ACQUIRED );
            t2.acquire( LockMgr::SHARED_RECORD, 2, ACQUIRED );
            t2.acquire( LockMgr::EXCLUSIVE_RECORD, 1, BLOCKED );
	    // t1's request would form a dependency cycle, so it should abort
            t1.acquire( LockMgr::EXCLUSIVE_RECORD, 2, ABORTED );
            t2.wakened( ); // with t1's locks released, t2 should wake
            t2.release( LockMgr::EXCLUSIVE_RECORD, 1);
            t2.release( LockMgr::SHARED_RECORD, 2);


            // Downgrades

            t1.acquire( LockMgr::EXCLUSIVE_RECORD, 1, ACQUIRED );
            t1.acquire( LockMgr::SHARED_RECORD, 1, ACQUIRED );
	    // t1 still has exclusive on resource 1, so t2 must wait
            t2.acquire( LockMgr::SHARED_RECORD, 1, BLOCKED );
            t1.release( LockMgr::EXCLUSIVE_RECORD, 1 );
            t2.wakened( ); // with the exclusive lock released, t2 wakes
            t1.release( LockMgr::SHARED_RECORD, 1);
            t2.release( LockMgr::SHARED_RECORD, 1);

            t1.acquire( LockMgr::EXCLUSIVE_RECORD, 1, ACQUIRED );
            t1.acquire( LockMgr::SHARED_RECORD, 1, ACQUIRED );
	    // t1 still has exclusive on resource 1, so t2 must wait
            t2.acquire( LockMgr::SHARED_RECORD, 1, BLOCKED );
            t1.release( LockMgr::SHARED_RECORD, 1);
	    // with t1 still holding exclusive on resource 1, t2 still blocked
            t1.release( LockMgr::EXCLUSIVE_RECORD, 1 );
            t2.wakened( ); // with the exclusive lock released, t2 wakes
            t2.release( LockMgr::SHARED_RECORD, 1);

            t1.acquire( LockMgr::EXCLUSIVE_RECORD, 1, ACQUIRED );
	    // t1 still has exclusive on resource 1, so t2 must wait
            t2.acquire( LockMgr::SHARED_RECORD, 1, BLOCKED );
	    // even though t2 is waiting for resource 1, t1 can still use it shared,
	    // because it already owns exclusive lock and can't block on itself
            t1.acquire( LockMgr::SHARED_RECORD, 1, ACQUIRED );
            t1.release( LockMgr::EXCLUSIVE_RECORD, 1 );
            t2.wakened( ); // with the exclusive lock released, t2 wakes
            t1.release( LockMgr::SHARED_RECORD, 1);
            t2.release( LockMgr::SHARED_RECORD, 1);

            t1.acquire( LockMgr::EXCLUSIVE_RECORD, 1, ACQUIRED );
            t1.acquire( LockMgr::SHARED_RECORD, 1, ACQUIRED );
            t2.acquire( LockMgr::EXCLUSIVE_RECORD, 1, BLOCKED );
            t1.release( LockMgr::EXCLUSIVE_RECORD, 1 );
            t1.release( LockMgr::SHARED_RECORD, 1);
            t2.wakened( );
            t2.release( LockMgr::EXCLUSIVE_RECORD, 1);

            t1.acquire( LockMgr::EXCLUSIVE_RECORD, 1, ACQUIRED );
            t2.acquire( LockMgr::EXCLUSIVE_RECORD, 1, BLOCKED );
            t1.acquire( LockMgr::SHARED_RECORD, 1, ACQUIRED );
            t1.release( LockMgr::EXCLUSIVE_RECORD, 1 );
            t1.release( LockMgr::SHARED_RECORD, 1);
            t2.wakened( );
            t2.release( LockMgr::EXCLUSIVE_RECORD, 1);
	

            // Upgrades

            t1.acquire( LockMgr::SHARED_RECORD, 1, ACQUIRED );
            t1.acquire( LockMgr::EXCLUSIVE_RECORD, 1, ACQUIRED ); // upgrade
            t2.acquire( LockMgr::SHARED_RECORD, 1, BLOCKED );
            t1.release( LockMgr::EXCLUSIVE_RECORD, 1 );
            t2.wakened( );
            t1.release( LockMgr::SHARED_RECORD, 1);
            t2.release( LockMgr::SHARED_RECORD, 1);

            t1.acquire( LockMgr::SHARED_RECORD, 1, ACQUIRED );
            t2.acquire( LockMgr::SHARED_RECORD, 1, ACQUIRED );
	    // t1 can't use resource 1 exclusively yet, because t2 is using it
            t1.acquire( LockMgr::EXCLUSIVE_RECORD, 1, BLOCKED );
            t2.release( LockMgr::SHARED_RECORD, 1);
            t1.wakened( ); // with t2's shared lock released, t1 wakes
            t1.release( LockMgr::EXCLUSIVE_RECORD, 1 );
            t1.release( LockMgr::SHARED_RECORD, 1);

            t1.quit( );
            t2.quit( );
        } catch ( ... ) {
            t1.quit( );
            t2.quit( );
            throw;
        }
    }

    TEST(LockMgrTest, TxPolicy) {

	{
	    // Test FirstComeFirstServe policy
	    LockMgr lm_first;
	    ClientTransaction t1( &lm_first, 1 );
	    ClientTransaction t2( &lm_first, 2 );
	    ClientTransaction t3( &lm_first, 3 );
	    try {
		// test1
		t1.acquire( LockMgr::EXCLUSIVE_RECORD, 1, ACQUIRED );
		t2.acquire( LockMgr::SHARED_RECORD, 1, BLOCKED );
		t3.acquire( LockMgr::EXCLUSIVE_RECORD, 1, BLOCKED );
		t1.release( LockMgr::EXCLUSIVE_RECORD, 1 );
		// t2 should wake first, because its request came before t3's
		t2.wakened( );
		t2.release( LockMgr::SHARED_RECORD, 1 );
		t3.wakened( );
		t3.release( LockMgr::EXCLUSIVE_RECORD, 1 );

		// test2
		t1.acquire( LockMgr::EXCLUSIVE_RECORD, 1, ACQUIRED );
		t3.acquire( LockMgr::EXCLUSIVE_RECORD, 1, BLOCKED );
		t2.acquire( LockMgr::SHARED_RECORD, 1, BLOCKED );
		t1.release( LockMgr::EXCLUSIVE_RECORD, 1 );
		// t3 should wake first, because its request came before t2's
		t3.wakened( );
		t3.release( LockMgr::EXCLUSIVE_RECORD, 1 );
		t2.wakened( );
		t2.release( LockMgr::SHARED_RECORD, 1 );

		t1.quit( );
		t2.quit( );
		t3.quit( );
	    } catch ( ... ) {
		t1.quit( );
		t2.quit( );
		t3.quit( );
		throw;
	    }
	}

	{
	    // Test READERS_FIRST policy
	    // shared request are considered read requests

	    LockMgr lm_readers(LockMgr::READERS_FIRST);
	    ClientTransaction t1( &lm_readers, 1 );
	    ClientTransaction t2( &lm_readers, 2 );
	    ClientTransaction t3( &lm_readers, 3 );
	    try {
		t1.acquire( LockMgr::EXCLUSIVE_RECORD, 1, ACQUIRED );
		t3.acquire( LockMgr::EXCLUSIVE_RECORD, 1, BLOCKED );
		t2.acquire( LockMgr::SHARED_RECORD, 1, BLOCKED );
		t1.release( LockMgr::EXCLUSIVE_RECORD, 1 );

		// t2 should wake first, even though t3 came first in time
		// because t2 is a reader and t3 is a writer
		t2.wakened( );
		t2.release( LockMgr::SHARED_RECORD, 1 );
		t3.wakened( );
		t3.release( LockMgr::EXCLUSIVE_RECORD, 1 );

		t1.quit( );
		t2.quit( );
		t3.quit( );
	    } catch ( ... ) {
		t1.quit( );
		t2.quit( );
		t3.quit( );
		throw;
	    }
	}

	{
	    // Test OLDEST_TX_FIRST policy
	    // for now, smaller TxIds are considered older

	    LockMgr lm_oldest(LockMgr::OLDEST_TX_FIRST);
	    ClientTransaction t1( &lm_oldest, 1 );
	    ClientTransaction t2( &lm_oldest, 2 );
	    ClientTransaction t3( &lm_oldest, 3 );
	    try {
		// test 1
		t1.acquire( LockMgr::EXCLUSIVE_RECORD, 1, ACQUIRED );
		t3.acquire( LockMgr::EXCLUSIVE_RECORD, 1, BLOCKED );
		t2.acquire( LockMgr::SHARED_RECORD, 1, BLOCKED );
		t1.release( LockMgr::EXCLUSIVE_RECORD, 1 );

		// t2 should wake first, even though t3 came first in time
		// because t2 is older than t3
		t2.wakened( );
		t2.release( LockMgr::SHARED_RECORD, 1 );
		t3.wakened( );
		t3.release( LockMgr::EXCLUSIVE_RECORD, 1 );

		// test 2
		t1.acquire( LockMgr::EXCLUSIVE_RECORD, 1, ACQUIRED );
		t2.acquire( LockMgr::SHARED_RECORD, 1, BLOCKED );
		t3.acquire( LockMgr::EXCLUSIVE_RECORD, 1, BLOCKED );
		t1.release( LockMgr::EXCLUSIVE_RECORD, 1 );

		// t2 should wake first, because it's older than t3
		t2.wakened( );
		t2.release( LockMgr::SHARED_RECORD, 1 );
		t3.wakened( );
		t3.release( LockMgr::EXCLUSIVE_RECORD, 1 );

		t1.quit( );
		t2.quit( );
		t3.quit( );
	    } catch ( ... ) {
		t1.quit( );
		t2.quit( );
		t3.quit( );
		throw;
	    }
	}

	{
	    LockMgr lm_blockers(LockMgr::BIGGEST_BLOCKER_FIRST);
	    ClientTransaction t1( &lm_blockers, 1 );
	    ClientTransaction t2( &lm_blockers, 2 );
	    ClientTransaction t3( &lm_blockers, 3 );
	    ClientTransaction t4( &lm_blockers, 4 );
	    try {
		// BIGGEST_BLOCKER_FIRST policy

		// set up t3 as the biggest blocker
		t3.acquire( LockMgr::EXCLUSIVE_RECORD, 2, ACQUIRED );
		t4.acquire( LockMgr::EXCLUSIVE_RECORD, 2, BLOCKED );

		// test 1
		t1.acquire( LockMgr::EXCLUSIVE_RECORD, 1, ACQUIRED );
		t3.acquire( LockMgr::EXCLUSIVE_RECORD, 1, BLOCKED );
		t2.acquire( LockMgr::SHARED_RECORD, 1, BLOCKED );
		t1.release( LockMgr::EXCLUSIVE_RECORD, 1 );
		// t3 should wake first, because it's a bigger blocker than t2
		t3.wakened( );
		t3.release( LockMgr::EXCLUSIVE_RECORD, 1 );
		t2.wakened( );
		t2.release( LockMgr::SHARED_RECORD, 1 );

		// test 2
		t1.acquire( LockMgr::EXCLUSIVE_RECORD, 1, ACQUIRED );
		t2.acquire( LockMgr::SHARED_RECORD, 1, BLOCKED );
		t3.acquire( LockMgr::EXCLUSIVE_RECORD, 1, BLOCKED );
		t1.release( LockMgr::EXCLUSIVE_RECORD, 1 );
		// t3 should wake first, even though t2 came first,
		// because it's a bigger blocker than t2
		t3.wakened( );
		t3.release( LockMgr::EXCLUSIVE_RECORD, 1 );
		t2.wakened( );
		t2.release( LockMgr::SHARED_RECORD, 1 );

		t3.release( LockMgr::EXCLUSIVE_RECORD, 2 );
		t4.wakened( );
		t4.release( LockMgr::EXCLUSIVE_RECORD, 2 );

		t1.quit( );
		t2.quit( );
		t3.quit( );
		t4.quit( );
	    } catch ( ... ) {
		t1.quit( );
		t2.quit( );
		t3.quit( );
		t4.quit( );
		throw;
	    }
	}
    }
}
