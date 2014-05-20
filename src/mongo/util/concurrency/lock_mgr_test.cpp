// lock_mgr_test.cpp

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
#if 0
	    TxResponse* rsp = _rsp.consume( );
	    ASSERT( AWAKENED == rsp->rspCode );
#endif
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
		    _lm->acquire(_xid, req->mode, req->store, req->recId, this);
		    _rsp.post(ACQUIRED);
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
                    log() << "t" << _xid << ": in QUIT" << endl;
		default:
                    more = false;
		    break;
		}
	    }
            log() << "t" << _xid << ": ending" << endl;
	}

	// inherited from Notifier, used by LockMgr::acquire
	virtual void operator()(const TxId& blocker) {
	    log() << "in sleep notifier" << endl;
	    _rsp.post(BLOCKED);
	}

    private:
	TxCommandBuffer _cmd;
	TxResponseBuffer _rsp;
	LockMgr* _lm;
	TxId _xid;
	boost::thread _thr;
	
    };

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

    TEST(LockMgrTest, SimpleTwoTx) {
	LockMgr lm;
	ClientTransaction t1( &lm, 1 );
	ClientTransaction t2( &lm, 2 );

	// no conflicts with shared locks on same/different objects

	t1.acquire( LockMgr::SHARED_RECORD, 1, ACQUIRED );
	t2.acquire( LockMgr::SHARED_RECORD, 2, ACQUIRED );
	t1.acquire( LockMgr::SHARED_RECORD, 2, ACQUIRED );
	t2.acquire( LockMgr::SHARED_RECORD, 1, ACQUIRED );

	t1.release( LockMgr::SHARED_RECORD, 1 );
	t1.release( LockMgr::SHARED_RECORD, 2 );
	t2.release( LockMgr::SHARED_RECORD, 1 );
	t2.release( LockMgr::SHARED_RECORD, 2 );


	// simple lock conflicts

	t1.acquire( LockMgr::SHARED_RECORD, 1, ACQUIRED );
	t2.acquire( LockMgr::EXCLUSIVE_RECORD, 1, BLOCKED );
	t1.release( LockMgr::SHARED_RECORD, 1 );
	t2.wakened( );
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


	// deadlock

	t1.acquire( LockMgr::SHARED_RECORD, 1, ACQUIRED );
	t2.acquire( LockMgr::SHARED_RECORD, 2, ACQUIRED );
	t1.acquire( LockMgr::EXCLUSIVE_RECORD, 2, BLOCKED );
	t2.acquire( LockMgr::EXCLUSIVE_RECORD, 1, ABORTED );
	t1.wakened( );
	t1.release( LockMgr::EXCLUSIVE_RECORD, 2);
	t1.release( LockMgr::SHARED_RECORD, 1);

	t1.acquire( LockMgr::SHARED_RECORD, 1, ACQUIRED );
	t2.acquire( LockMgr::SHARED_RECORD, 2, ACQUIRED );
	t2.acquire( LockMgr::EXCLUSIVE_RECORD, 1, BLOCKED );
	t1.acquire( LockMgr::EXCLUSIVE_RECORD, 2, ABORTED );
	t2.wakened( );
	t2.release( LockMgr::EXCLUSIVE_RECORD, 1);
	t2.release( LockMgr::SHARED_RECORD, 2);


	// Downgrades

	t1.acquire( LockMgr::EXCLUSIVE_RECORD, 1, ACQUIRED );
	t1.acquire( LockMgr::SHARED_RECORD, 1, ACQUIRED );
	t2.acquire( LockMgr::SHARED_RECORD, 1, BLOCKED );
	t1.release( LockMgr::EXCLUSIVE_RECORD, 1 );
	t2.wakened( );
	t1.release( LockMgr::SHARED_RECORD, 1);
	t2.release( LockMgr::SHARED_RECORD, 1);

	t1.acquire( LockMgr::EXCLUSIVE_RECORD, 1, ACQUIRED );
	t2.acquire( LockMgr::SHARED_RECORD, 1, BLOCKED );
	t1.acquire( LockMgr::SHARED_RECORD, 1, ACQUIRED );
	t1.release( LockMgr::EXCLUSIVE_RECORD, 1 );
	t2.wakened( );
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
	t1.acquire( LockMgr::EXCLUSIVE_RECORD, 1, ACQUIRED );
	t2.acquire( LockMgr::SHARED_RECORD, 1, BLOCKED );
	t1.release( LockMgr::EXCLUSIVE_RECORD, 1 );
	t2.wakened( );
	t1.release( LockMgr::SHARED_RECORD, 1);
	t2.release( LockMgr::SHARED_RECORD, 1);

	t1.acquire( LockMgr::SHARED_RECORD, 1, ACQUIRED );
	t2.acquire( LockMgr::SHARED_RECORD, 1, ACQUIRED );
	t1.acquire( LockMgr::EXCLUSIVE_RECORD, 1, BLOCKED );
	t2.release( LockMgr::SHARED_RECORD, 1);
	t1.wakened( );
	t1.release( LockMgr::EXCLUSIVE_RECORD, 1 );
	t1.release( LockMgr::SHARED_RECORD, 1);

        t1.quit( );
        t2.quit( );
    }
}
