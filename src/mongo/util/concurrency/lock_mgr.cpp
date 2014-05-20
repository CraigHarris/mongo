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

#include <boost/thread/locks.hpp>
#include "mongo/util/assert_util.h"
#include "mongo/util/log.h"
#include "mongo/util/concurrency/lock_mgr.h"

using namespace std;
using namespace boost;
using namespace mongo;

LockMgr::LockId LockMgr::LockRequest::nextLid = 0;

LockMgr::LockRequest::LockRequest( const TxId& xid,
				   const LockMode& mode,
				   const RecordStore* store)
    : sleep(false), lid(nextLid++), xid(xid), mode(mode), store(store) { }

LockMgr::LockRequest::LockRequest( const TxId& xid,
				   const LockMode& mode,
				   const RecordStore* store,
				   const RecordId& recId)
    : sleep(false), lid(nextLid++), xid(xid), mode(mode), store(store), recId(recId) { }

LockMgr::LockRequest::~LockRequest( ) { }

bool LockMgr::LockRequest::matches( const TxId& xid,
				    const LockMode& mode,
				    const RecordStore* store,
				    const RecordId& recId ) {
    return
	this->xid == xid &&
	this->mode == mode &&
	this->store == store &&
	this->recId == recId;
}

namespace {

    bool isCompatible(const LockMgr::LockMode& mode1, const LockMgr::LockMode& mode2) {
        return mode1==mode2 && (LockMgr::SHARED_RECORD==mode1 || LockMgr::SHARED_STORE==mode1);
    }
}

LockMgr::LockMgr( const LockingPolicy& policy ) : _policy(policy), _guard() { }

LockMgr::~LockMgr( ) {
    unique_lock<boost::mutex> guard(_guard);
    for(map<LockId,LockRequest*>::iterator locks = _locks.begin();
        locks != _locks.end(); ++locks) {
        delete locks->second;
    }
}

bool LockMgr::isLocked( const TxId& holder,
                        const LockMode& mode,
                        const RecordStore* store) {
    unique_lock<boost::mutex> guard(_guard);
    map<const RecordStore*, list<LockId>*>::iterator locks = _containerLocks.begin();
    if (locks == _containerLocks.end()) { return false; }
    for (list<LockId>::iterator nextLockId = locks->second->begin();
         nextLockId != locks->second->end(); ++nextLockId) {
        LockRequest* nextLockRequest = _locks[*nextLockId];
        if (nextLockRequest->xid == holder && nextLockRequest->mode == mode) {
            return true;
        }
    }
    return false;
}

bool LockMgr::isLocked( const TxId& holder,
                        const LockMode& mode,
                        const RecordStore* store,
                        const RecordId& recId) {
    unique_lock<boost::mutex> guard(_guard);
    map<const RecordStore*, map<RecordId, list<LockId>*> >::iterator storeLocks = _recordLocks.begin();
    if (storeLocks == _recordLocks.end()) { return false; }
    map<RecordId, list<LockId>*>::iterator recordLocks = storeLocks->second.begin();
    if (recordLocks == storeLocks->second.end()) { return false; }
    for (list<LockId>::iterator nextLockId = recordLocks->second->begin();
         nextLockId != recordLocks->second->end(); ++nextLockId) {
        LockRequest* nextLockRequest = _locks[*nextLockId];
        if (nextLockRequest->xid == holder && nextLockRequest->mode == mode) {
            return true;
        }
    }
    return false;
}

void LockMgr::addLockToQueueUsingPolicy( LockMgr::LockRequest* lr ) {
    list<LockId>* queue = _recordLocks[lr->store][lr->recId];
    list<LockId>::iterator nextLockId = queue->begin();
    switch (_policy) {
    case FIRST_COME:
        queue->push_back(lr->lid);
        return;
    case READERS_FIRST:
        if (LockMgr::EXCLUSIVE_RECORD == lr->mode) {
            queue->push_back(lr->lid);
            return;
        }
        for (; nextLockId != queue->end(); ++nextLockId) {
            LockMgr::LockRequest* nextRequest = _locks[*nextLockId];
            if (LockMgr::EXCLUSIVE_RECORD==nextRequest->mode) {
                queue->insert(nextLockId, lr->lid);
                return;
            }
        }
        break;
    case OLDEST_TX_FIRST:
        for (; nextLockId != queue->end(); ++nextLockId) {
            LockMgr::LockRequest* nextRequest = _locks[*nextLockId];
            if (lr->xid > nextRequest->xid) {
                queue->insert(nextLockId, lr->lid);
                return;
            }
        }
        break;
    case BIGGEST_BLOCKER_FIRST: {
        map<TxId,set<TxId>*>::iterator lrWaiters = _waiters.find(lr->xid);
        size_t lrNumWaiters = (lrWaiters == _waiters.end()) ? 0 : lrWaiters->second->size();
        for (; nextLockId != queue->end(); ++nextLockId) {
            LockMgr::LockRequest* nextRequest = _locks[*nextLockId];
            map<TxId,set<TxId>*>::iterator nextRequestWaiters = _waiters.find(nextRequest->xid);
            size_t nextRequestNumWaiters = (nextRequestWaiters == _waiters.end())
                                           ? 0 : nextRequestWaiters->second->size();
            if (lrNumWaiters > nextRequestNumWaiters) {
                queue->insert(nextLockId, lr->lid);
                return;
            }
        }
        break;
    }
    case QUICKEST_TO_FINISH:
        // XXX eventually, get stats from cached plans?
        break;
    }

    queue->push_back(lr->lid);
}

void LockMgr::abort(const TxId& goner) {
    // cleanup locks
    map<TxId, set<LockId>*>::iterator locks = _xaLocks.find(goner);
    invariant(locks != _xaLocks.end());
    for (set<LockId>::iterator nextLockId = locks->second->begin();
         nextLockId != locks->second->end(); ++nextLockId) {
        map<LockId,LockRequest*>::iterator nextLock = _locks.find(*nextLockId);
        invariant(nextLock != _locks.end());
        LockRequest* nextLockRequest = nextLock->second;

	if (LockMgr::SHARED_STORE == nextLockRequest->mode ||
	    LockMgr::EXCLUSIVE_STORE == nextLockRequest->mode) {
	    release(goner, nextLockRequest->mode, nextLockRequest->store);
	}
	else {
	    release(goner, nextLockRequest->mode, nextLockRequest->store, nextLockRequest->recId);
	}
    }

    throw AbortException();
}

void LockMgr::acquire(const TxId& requestor,
                      const LockMode& mode,
                      const RecordStore* store) {
#if 0
    unique_lock<mutex> guard(_guard);

    list<LockId>::iterator locks = _containerLocks.find(store);
    while (locks != _containerLocks.end()) {
	Lock lock = locks->second;
	if () {
	    TxId goner;
	    if (detect_deadlock(xa.getXid(), &goner)) {
		goner->abort();
		continue;
	    }
	    _waiters[lock.holder] = xa.getXid();
	    lock.wait(guard,
		      [mode, lock]()->bool{
			  return noLock ==
                              lock.mode || (LockMgr::SHARED_RECORD == lock.mode && lock.mode == mode);
		      });
	}
    }

    LockRequest* lr = new LockRequest(requestor, mode, store);
    _locks[lr->lid] = lr;

    // add lock request to list of request for recId
    map<RecordStore,list<LockId>*> rec_iter = _containerLocks.find(recId);
    if (rec_iter == _containerLocks.end()) {
	list<LockId>* containerLocks = new list<LockId>();
	containerLocks.push_back(_nextLockId);
	_containerLocks[recId] = containerLocks;
    }
    else {
	rec_iter->second->push_back(_nextLockId);
    }

    // add lock request to set of requests of requesting TxId
    std::map<TxId, std::set<LockId>*>::iterator xa_iter = _xaLocks.find(requestor);
    if (xa_iter == _xaLocks.end()) {
	set<TxId>* myLocks = new set<LockId>();
	myLocks->insert(_nextLockId);
	_xaLocks[requestor] = myLocks;
    }
    else {
	xa_iter->second->insert(_nextLockId);
    }
#endif
}

void LockMgr::acquire( const TxId& requestor,
                       const LockMode& mode,
                       const RecordStore* store,
                       const RecordId& recId,
                       Notifier* sleepNotifier ) {

    // first, acquire a lock on the store
    acquire(requestor, LockMgr::SHARED_STORE, store);

    // create the lock request and add to TxId's set of lock requests
    unique_lock<boost::mutex> guard(_guard);

    LockRequest* lr = new LockRequest(requestor, mode, store, recId);
    _locks[lr->lid] = lr;

    // add lock request to set of requests of requesting TxId
    map<TxId,set<LockId>*>::iterator xa_iter = _xaLocks.find(requestor);
    if (xa_iter == _xaLocks.end()) {
	set<TxId>* myLocks = new set<LockId>();
	myLocks->insert(lr->lid);
	_xaLocks[requestor] = myLocks;
    }
    else {
	xa_iter->second->insert(lr->lid);
    }

    // if this is the 1st lock request against this store, or recId, init and exit
    map<const RecordStore*, map<RecordId,list<LockId>*> >::iterator storeLocks = _recordLocks.find(store);
    if (storeLocks == _recordLocks.end()) {
        map<RecordId, list<LockId>*> recordsLocks;
        list<LockId>* locks = new list<LockId>();
        locks->push_back(lr->lid);
        recordsLocks[recId] = locks;
        _recordLocks[store] = recordsLocks;
        return;
    }

    map<RecordId,list<LockId>*>::iterator recLocks = storeLocks->second.find(recId);
    if (recLocks == storeLocks->second.end()) {
        list<LockId>* locks = new list<LockId>();
        locks->push_back(lr->lid);
        storeLocks->second.insert(pair<RecordId,list<LockId>*>(recId,locks));
        return;
    }

    // check to see if requestor has already locked recId
    list<LockId>* queue = recLocks->second;
    for (list<LockId>::iterator nextLockId = queue->begin();
         nextLockId != queue->end(); ++nextLockId) {
        LockRequest* nextRequest = _locks[*nextLockId];
        if (nextRequest->xid != requestor) {
            continue;
        }

        // nextRequest is owned by requestor

        if (nextRequest->mode == mode) {
            // we already have the lock, don't need the one we created above
            _xaLocks[requestor]->erase(lr->lid);
            _locks.erase(lr->lid);
            delete lr;
            return;
        }
        else if (LockMgr::EXCLUSIVE_RECORD == nextRequest->mode) {
            // downgrade: we're asking for a SHARED_RECORD lock and have an EXCLUSIVE
            // since we're not blocked, the EXCLUSIVE must be at the front
            invariant(nextLockId == queue->begin());

            // add our SHARED_RECORD request immediately after
            // our EXCLUSIVE_RECORD request, so that when we
            // release the EXCLUSIVE, we'll be granted the SHARED
            //
            queue->insert(++nextLockId, lr->lid);
            return;
        }

        // we're asking for an upgrade.  this is ok if there are
        // no other upgrade requests.
        //
        // the problem with other upgrade requests is that our view
        // of the record may change while we have a shared lock, but
        // before we're granted the exclusive lock.

        set<TxId> otherSharers;
        list<LockId>::iterator nextSharerId = queue->begin();
        for (; nextSharerId != queue->end(); ++nextSharerId) {
            if (*nextSharerId == nextRequest->lid) continue; // skip 
            LockRequest* nextSharer = _locks[*nextSharerId];
            if (LockMgr::EXCLUSIVE_RECORD == nextSharer->mode) {
                set<TxId>::iterator nextUpgrader = otherSharers.find(nextSharer->xid);
                if (nextUpgrader != otherSharers.end() && *nextUpgrader != requestor) {
                    abort(requestor);
                    return; // abort throws
                }
                break;
            }
            otherSharers.insert(nextSharer->xid);
        }

        // safe to upgrade
        queue->insert(nextSharerId, lr->lid);
        if (otherSharers.size() > 1) {
            // there are other sharers, so we block on their completion
        }
        return;
    }

    list<LockId>::iterator nextLockId = queue->begin();
    if (nextLockId != queue->end()) {
	LockRequest* nextLockRequest = _locks[*nextLockId];
        if (! isCompatible(mode, nextLockRequest->mode)) {
	    const TxId& blocker = nextLockRequest->xid;

            // check for deadlock
	    map<TxId, set<TxId>*>::iterator requestorsWaiters = _waiters.find(requestor);
	    if (requestorsWaiters != _waiters.end()) {
		if (requestorsWaiters->second->find(blocker) != requestorsWaiters->second->end()) {
		    // the transaction that would block requestor is already blocked by requestor
		    // if requestor waited for blocker there would be a deadlock
		    //
		    abort(requestor);
		}
            }

	    // set up for future deadlock detection
	    // add requestor to nextLockRequest's waiters
	    //
	    map<TxId, set<TxId>*>::iterator blockersWaiters = _waiters.find(blocker);
	    set<TxId>* waiters;
	    if (blockersWaiters == _waiters.end()) {
		waiters = new set<TxId>();
		_waiters[nextLockRequest->xid] = waiters;
	    }
	    else {
		waiters = blockersWaiters->second;
	    }

	    waiters->insert(requestor);
	    if (requestorsWaiters != _waiters.end()) {
		waiters->insert(requestorsWaiters->second->begin(),
				requestorsWaiters->second->end());
	    }

            // add our request to the queue
            addLockToQueueUsingPolicy( lr );

            // call the sleep notification function
	    if (NULL != sleepNotifier) {
		log() << "before sleep notifier" << endl;
		(*sleepNotifier)(blocker);
	    } else {
		log() << "NULL sleep notifier" << endl;
	    }

            // block
	    lr->sleep = true;
	    while (lr->sleep) {
		lr->lock.wait(guard);
	    }

	    // when awakened, remove ourselves from the set of waiters
            _waiters[nextLockRequest->xid]->erase(requestor);
            return;
        }
    }

    // add our request to the queue
    addLockToQueueUsingPolicy( lr );
}

void LockMgr::release( const TxId& holder ) {
    map<TxId, set<LockId>*>::iterator lockIdsHeld = _xaLocks.find(holder);
    if (lockIdsHeld == _xaLocks.end()) { return; }
    for (set<LockId>::iterator nextLockId = lockIdsHeld->second->begin();
	 nextLockId != lockIdsHeld->second->end(); ++nextLockId) {
	LockRequest* nextLock = _locks[*nextLockId];
	release( holder, nextLock->mode, nextLock->store, nextLock->recId );
    }
}

void LockMgr::release( const TxId& holder,
                       const LockMode& mode,
                       const RecordStore* store ) {
#if 0
    unique_lock<boost::mutex> guard(_guard);

    // find the lock to release
    const LockId& lockId = _containerLocks[store]->front();

    // clean up the lock information
    //
    LockRequest* lockRequest = _locks[lockId];
    delete lockRequest;
    _xaLocks[holder]->erase(lockId);
    _containerLocks[store]->pop_front();
    _locks.erase(lockId);

    // deal with transactions that were waiting for this lock to be released
    //
    list<LockId>::iterator nextWaiter = _containerLocks[store]->find();
    LockRequest* newOwner = _locks[*nextWaiter];
    for (; nextWaiter == _containerLocks[recId]->end(); ++nextWaiter) {
        LockRequest* nextSleeper = _locks[*nextWaiter];
        if (! isCompatible(nextSleeper->mode, newOwner->mode)) { return; }
	nextSleeper->sleep = false;
        nextSleeper->lock.notify_one();
    }
#endif
}

void LockMgr::release( const TxId& holder,
                       const LockMode& mode,
                       const RecordStore* store,
                       const RecordId& recId) {
    {
        unique_lock<boost::mutex> guard(_guard);

        bool seenExclusive = false;
        bool foundLock = false;
        set<TxId> otherSharers;

        list<LockId>* queue = _recordLocks[store][recId];
        list<LockId>::iterator nextLockId = queue->begin();
        LockRequest* ourLock = NULL;

        // find the lock to release
        for( ; !foundLock && nextLockId != queue->end(); ++nextLockId) {
            LockRequest* nextLock = _locks[*nextLockId];
            if (! nextLock->matches(holder, mode, store, recId)) {
                if (LockMgr::SHARED_RECORD == nextLock->mode && !seenExclusive)
                    otherSharers.insert(nextLock->xid);
                else
                    seenExclusive = true;
            }
            else {
                // this is our lock.  remove it
                //
                ourLock = nextLock;
                _xaLocks[holder]->erase(*nextLockId);
                _locks.erase(*nextLockId);
                queue->erase(nextLockId++); 
                delete ourLock;

                foundLock = true;
                break; // don't increment nextLockId again
            }
        }

        invariant(foundLock);

        // deal with transactions that were waiting for this lock to be released
        //
        if (LockMgr::EXCLUSIVE_RECORD == mode) {
#if 0
            // this should be true unless we've upgraded
            // perhaps we should assert that if there is a first lock on the queue
            // that it should be owned by holder?
            invariant(nextLockId == queue->begin());
#endif
            bool seenSharers = false;
            while (++nextLockId != queue->end()) {
                LockRequest* nextSleeper = _locks[*nextLockId];
                if (LockMgr::EXCLUSIVE_RECORD == nextSleeper->mode) {
                    if (!seenSharers)
			nextSleeper->sleep = false;
                        nextSleeper->lock.notify_one();
                    break;
                }
                else {
                    seenSharers = true;
		    nextSleeper->sleep = false;
                    nextSleeper->lock.notify_one();
                }
            }
        }
        else if (!seenExclusive) {
            // we were one of possibly many SHARED_RECORD lock holders.
            // continue iterating, until we find an exclusive lock
            for (;nextLockId != queue->end(); ++nextLockId) {
                LockRequest* nextLock = _locks[*nextLockId];
                if (LockMgr::SHARED_RECORD == nextLock->mode) {
                    otherSharers.insert(nextLock->xid);
                    continue;
                }

                // first exclusive lock request found. it's blocked.
                // notify it if there are no otherSharers
                //
                // it's possible the Exclusive locker previously held
                // a shared lock, then upgraded and blocked
                // 
                otherSharers.erase(nextLock->xid);
                if (0 == otherSharers.size()) {
                    // no other sharers, awaken the exclusive locker
		    nextLock->sleep = false;
                    nextLock->lock.notify_one();
                }
                break;
            }
        }
        else {
            // we're releasing a readlock that was blocked
            // this can only happen if we're aborting
        }
    }

    // finally, release the sharedContainerLock
    release(holder, SHARED_STORE, store);
}
