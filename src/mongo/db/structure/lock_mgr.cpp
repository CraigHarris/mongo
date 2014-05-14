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

#include "mongo/db/structure/lock_mgr.h"
#include "mongo/util/assert_util.h"

using namespace std;
using namespace mongo;

LockMgr::LockRequest::LockRequest( const TxId& xid,
                          const LockMode& mode,
                          const RecordStore* store)
    : lid(nextLid++), xid(xid), mode(mode), store(store) { }

LockMgr::LockRequest::LockRequest( const TxId& xid,
                          const LockMode& mode,
                          const RecordStore* store,
                          const RecordId& recId)
    : lid(nextLid++), xid(xid), mode(mode), store(store), recId(recId) { }

LockMgr::LockRequest::~LockRequest( ) { }

namespace {

    bool isCompatible(const LockMgr::LockMode& mode1, const LockMgr::LockMode& mode2) {
        return mode1==mode2 && (LockMgr::SHARED_RECORD==mode1 || LockMgr::SHARED_STORE==mode1);
    }

    bool deadlockHelper(set<TxId>* seen, set<TxId>* check) {
        set<TxId> newToCheck;
        for (set<TxId>::iterator toCheck = check->begin(); toCheck != check->end(); ++toCheck) {
            TxId nextToCheck = *toCheck;
            if (seen->find(nextToCheck) != seen->end()) {
                return true;
            }
            seen->insert(nextToCheck);
            map<TxId,set<TxId>*>::iterator waiter_iter = _waiters.find(nextToCheck);
            if (waiter_iter != _waiters.end()) {
                for (set<TxId>::iterator waiters = waiters_iter->second.begin();
                 waiters != _waiters.end(); ++waiters) {
                    newToCheck.insert(*waiters);
                }
            }
        }
        return deadlockHelper(seen, &newToCheck);
    }
}

LockMgr::LockMgr( const LockingPolicy& policy ) : _policy(policy), _mutex("LockMgr") { }

LockMgr::~LockMgr( ) {
    for(map<LockId,LockRequest*>::iterator locks = _locks.begin();
        locks != _locks.end(); ++locks) {
        delete locks->second;
    }
}

void LockMgr::addLockToQueueUsingPolicy( LockMgr::LockRequest* lr ) {
    list<LockId>* queue = _recordLocks[lr->store][lr->rid];
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
        size_t lrNumWaiters = (lrWatiers == _waiters.end()) ? 0 : lrWaiters->second->size();
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

    queue->push_back(lr);
}

bool LockMgr::detectDeadlock(const TxId& acquirer, TxId* outGoner) {
    set<TxId> check;
    set<TxId> seen;

    seen.insert(acquirer);
    for (iterator<TxId> waiters = _waiters.begin(); waiters != _waiters.end(); ++waiters) {
	check.insert(*waiters);
    }
    if (deadlockHelper(&seen, &check)) {
	*outGoner = acquirer;
	return true;
    }
    return false;
}

void LockMgr::abort(const TxId& goner) {
    // cleanup locks
    map<TxId, set<LockId>*>::iterator locks = _xaLocks.find(goner);
    for (set<LockId>::iterator nextLockId = locks->second->begin();
         nextLockId != locks->second->end(); ++nextLockId) {
        map<LockId,LockRequest*>::iterator nextLock = _locks.find(*nextLockId);
        invariant(nextLock != _locks.end());
        LockRequest* nextLockRequest = nextLock->second;

        vector<LockId>* recLocks = _recordLocks[nextLock->store][nextLock->recId];
        LockRequest* recLockOwner = recLocks->front();
        if (recLockOwner->xid == goner) {
            for (vector<LockId>::iterator nextRecLock = recLocks->begin();
                 nextRecLock != recLocks->end(); ++nextRecLock)
        }
        

        delete *nextLock;
        _locks.erase(*nextLockId);
        _xaLocks.erase(*nextLockId);
    }
}

void LockMgr::acquire(const TxId& requestor,
                      const LockMode& mode,
                      const RecordStore& store) {

    unique_lock<mongo::mutex> guard(_guard);

    auto locks = _containerLocks.find(store);
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

    LockRequest* lr = new LockRequest(requestor, mode, &store);
    _locks[lr->lid] = lr;

    // add lock request to vector of request for recId
    map<RecordStore,list<LockId>*> rec_iter = _containerLocks.find(recId);
    if (rec_iter == _containerLocks.end()) {
	vector<LockId>* containerLocks = new vector<LockId>();
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


    _nextLockId++;
}

void LockMgr::acquire( const TxId& requestor,
                       const LockMode& mode,
                       const RecordStore& store,
                       const RecordId& recId ) {

    // first, acquire a lock on the store
    acquire(requestor, LockMgr::SHARED_STORE, store);

    unique_lock<mutex> guard(_guard);

    LockRequest* lr = new LockRequest(requestor, mode store, recId);
    _locks[lr->lid] = lr;

    // see if we already have a lock on this record
    map<RecordId,vector<LockId>*>::iterator rec_iter = _recordLocks.find(recId);
    if (rec_iter != _recordLocks.end()) {
        list<LockId>* queue = rec_iter->second;
        for (list<LockId>::iterator nextLockId = queue->begin();
             nextLockId != queue->end(); ++nextLockId) {
            LockRequest* nextRequest = _locks[*nextLockId];
            if (nextRequest->xid == requestor) {
                if (nextRequest->mode == mode) {
                    _locks.erase(lr->id);
                    delete lr;
                    return; // we already have the lock
                }
                if (LockMgr::EXCLUSIVE_RECORD == nextRequest->mode) {
                    // since we're not blocked, we must be at the front
                    invariant(nextLockId == queue->begin());

                    // add our SHARED_RECORD request immediately after
                    // our EXCLUSIVE_RECORD request, so that when we
                    // release the EXCLUSIVE, we'll be granted the SHARED
                    //
                    _recordLocks.insert(++nextLockId, lr);
                    return;
                }

                // we're asking for an upgrade.  this is ok if there are
                // no other upgrade requests.
                //
                // the problem with other upgrade requests is that our view
                // of the record may change while we have a shared lock, but
                // before we're granted the exclusive lock.

                set<TxId> otherSharers;
                list<LockId::iterator nextSharerId = queue->begin();
                for (; nextSharerId != queue->end(); ++nextSharerId) {
                    if (*nextSharerId == nextRequest->lid) continue; // skip ourselves
                    LockRequest* nextSharer = _locks[*nextSharerId];
                    if (LockMgr::EXCLUSIVE_RECORD == nextSharer->mode) {
                        set<TxId>::iterator nextSharer = otherSharers.find(nextSharer->xid);
                        if (nextSharer != otherSharers.end() && *nextSharer != requestor) {
                            // we have to abort
                            release(requestor); // release our locks
                            throw "FIX-ME";
                        }
                        break;
                    }
                    otherSharers.insert(nextSharer->xid);
                }

                // safe to upgrade
                queue.insert(nextSharerId, lr );
                if (otherSharers.size() > 1) {
                    // there are other sharers, so we block on their completion
                }
                return;
            }
        }
    }

    // most of the time, there will be no locks on the desired record

    // add lock request to vector of request for recId
    auto rec_iter = _recordLocks.find(recId);
    if (rec_iter == _recordLocks.end()) {
	vector<LockId>* recLocks = new vector<LockId>();
	_recordLocks[recId] = recLocks;
	recLocks.push_back(_nextLockId);
    }
    else {
	rec_iter->second->push_back(_nextLockId);
    }

    // add lock request to set of requests of requesting TxId
    auto xa_iter = _xaLocks.find(requestor);
    if (xa_iter == _xaLocks.end()) {
	set<TxId>* myLocks = new set<LockId>();
	_xaLocks[requestor] = myLocks;
	myLocks->insert(_nextLockId);
    }
    else {
	xa_iter->second->insert(_nextLockId);
    }

    map<LockId,LockRequest*>::iterator locksOnRecord = _recordLocks.find(recId);
    if (locksOnRecord != _recordLocks.end()) {
        // see if we already have a lock
        
        vector<LockId>::iterator nextLockId = locksOnRecord->second.begin();
        if (!isCompatible(nextLockId, grantedLock)) {

            // deal with deadlock
            TxId goner;
            if (detect_deadlock(requestor, &goner)) {
                goner->abort();
                continue;
            }

            // block
            _waiters[lock.holder].insert(requestor);
            lock.wait(guard,
                      [mode, lock]()->bool{
                          return noLock ==
                              lock.mode || (LockMgr::SHARED_RECORD == lock.mode && lock.mode == mode);
                      });
            _waiters[lock.holder].erase(requestor);
        }
    }
}

void LockMgr::release( const TxId holder,
                       const LockMode& mode,
                       const RecordStore& store) {
    unique_lock<mutex> guard(_guard);

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
    std::vector<LockId>::iterator nextWaiter = _containerLocks[store]->find();
    LockRequest* newOwner = _locks[*nextWaiter];
    for (; nextWaiter == _containerLocks[recId]->end(); ++nextWaiter) {
        LockRequest* nextSleeper = _locks[*nextWaiter];
        if (! isCompatible(nextSleeper->mode, newOwner->mode)) { return; }
        nextSleeper->lock.notify_one(guard);
    }
}

void LockMgr::release( const TxId& holder,
                       const LockMode& mode,
                       const RecordStore& store,
                       const RecordId& recId) {
    {
        unique_lock<mutex> guard(_guard);

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
                queue->erase(nextLockId);
                delete ourLock;

                foundLock = true;
            }
        }

        invariant(foundLock);

        // deal with transactions that were waiting for this lock to be released
        //
        if (LockMgr::EXCLUSIVE_RECORD == mode) {
            invariant(nextLockId == queue->begin());
            bool seenSharers = false;
            while (++nextLockId != queue->end()) {
                LockRequest* nextSleeper = _locks[*nextLockId];
                if (LockMgr::EXCLUSIVE_RECORD == nextSleeper->mode) {
                    if (!seenSharers)
                        nextSleeper->lock.notify_one(guard);
                    break;
                }
                else {
                    seenSharers = true;
                    nextSleeper->lock.notify_one(guard);
                }
            }
        }
        else if (!seenExclusive) {
            // we were one of possibly many SHARED_RECORD lock holders.
            // continue iterating, until we find an exclusive lock
            while (++nextLockId != queue->end()) {
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
                    nextLock->lock.notify_one(guard);
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

