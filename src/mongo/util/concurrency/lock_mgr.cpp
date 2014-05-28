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
#include <sstream>
#include "mongo/util/assert_util.h"
#include "mongo/util/log.h"
#include "mongo/util/timer.h"
#include "mongo/util/concurrency/lock_mgr.h"

using namespace std;
using namespace boost;
using namespace mongo;

/*---------- LockStats functions ----------*/

LockMgr::LockStats::LockStats( const LockMgr::LockStats& other )
    : _numRequests(other._numRequests),
      _numPreexistingRequests(other._numPreexistingRequests),
      _numBlocks(other._numBlocks),
      _numDeadlocks(other._numDeadlocks),
      _numDowngrades(other._numDowngrades),
      _numUpgrades(other._numUpgrades),
      _numMillisBlocked(other._numMillisBlocked) { }

LockMgr::LockStats& LockMgr::LockStats::operator=( const LockMgr::LockStats& other ) {
    if (this != &other) {
        _numRequests = other._numRequests;
        _numPreexistingRequests = other._numPreexistingRequests;
        _numBlocks = other._numBlocks;
        _numDeadlocks = other._numDeadlocks;
        _numDowngrades = other._numDowngrades;
        _numUpgrades = other._numUpgrades;
        _numMillisBlocked = other._numMillisBlocked;
    }
    return *this;
}


/*---------- LockRequest functions ----------*/

LockMgr::LockId LockMgr::LockRequest::nextLid = 1; // a zero parentLid means no parent

LockMgr::LockRequest::LockRequest( const TxId& xid,
                                   const unsigned& mode,
                                   const ResourceId& container,
                                   const ResourceId& resId)
    : sleepCount(0),
      parentLid(0),
      lid(nextLid++),
      xid(xid),
      mode(mode),
      container(container),
      resId(resId),
      count(1) { }

LockMgr::LockRequest::~LockRequest( ) { }

bool LockMgr::LockRequest::matches( const TxId& xid,
                                    const unsigned& mode,
                                    const ResourceId& container,
                                    const ResourceId& resId ) {
    return
        this->xid == xid &&
        this->mode == mode &&
        this->container == container &&
        this->resId == resId;
}

string LockMgr::LockRequest::toString( ) const {
    stringstream result;
    result << "<lid:" << lid
           << ",parentLid: " << parentLid
           << ",xid:" << xid
           << ",mode:" << mode
           << ",resId:" << resId
           << ",container:" << container
           << ",count:" << count
           << ">";
    return result.str();
}

/*---------- Utility function ----------*/

namespace {

    bool isExclusive( const unsigned& mode, const unsigned level=0 ) {
        return LockMgr::kExclusive == (mode & (0x1 << level));
    }

    bool isShared( const unsigned& mode, const unsigned level=0 ) {
        return LockMgr::kShared == (mode & (0x1 << level));
    }

    bool isCompatible( const unsigned& mode1, const unsigned& mode2 ) {
        return mode1==mode2 && (isShared(mode1) || isShared(mode1));
    }

    bool isBlocked( const LockMgr::LockRequest* lr ) {
        return lr->sleepCount > 0;
    }

    bool shouldAwake( LockMgr::LockRequest* lr ) {
        return 0 == --lr->sleepCount;
    }
}

/*---------- LockMgr public functions (mutex guarded) ---------*/

unsigned const LockMgr::kShared;
unsigned const LockMgr::kExclusive;
LockMgr* LockMgr::_singleton = NULL;

LockMgr* LockMgr::getSingleton(const LockingPolicy& policy) {
    if (NULL == _singleton) {
        _singleton = new LockMgr(policy);
    }
    return _singleton;
}

LockMgr::LockMgr( const LockingPolicy& policy ) : _policy(policy), _guard() { }

LockMgr::~LockMgr( ) {
    unique_lock<boost::mutex> guard(_guard);
    for(map<LockId,LockRequest*>::iterator locks = _locks.begin();
        locks != _locks.end(); ++locks) {
        delete locks->second;
    }
}

void LockMgr::shutdown(const unsigned& millisToQuiesce = 1000 ) {
    unique_lock<boost::mutex> guard(_guard);
    _policy = LockMgr::SHUTDOWN;
    _millisToQuiesce = millisToQuiesce;
    _timer.millisReset();
}

void LockMgr::setPolicy( const LockingPolicy& policy ) {
    unique_lock<boost::mutex> guard(_guard);
    if (policy == _policy) return;

    _policy = policy;

    // if moving away from {READERS,WRITERS}_ONLY, awaken requests that were pending
    //
    if (LockMgr::READERS_ONLY == _policy || LockMgr::WRITERS_ONLY == _policy) {
        // Awaken requests that were blocked on the old policy
        // these are waiting on TxId 0
        map<TxId, set<LockId>*>::iterator lockIdsHeld = _xaLocks.find(0);
        if (lockIdsHeld != _xaLocks.end()) {
            for (set<LockId>::iterator nextLockId = lockIdsHeld->second->begin();
                 nextLockId != lockIdsHeld->second->end(); ++nextLockId) {
                LockRequest* nextLock = _locks[*nextLockId];
                if (shouldAwake(nextLock)) {
                    nextLock->lock.notify_one();
                }
            }
        }
    }

    // if moving to {READERS,WRITERS}_ONLY, block until no incompatible locks
    if (LockMgr::READERS_ONLY == policy || LockMgr::WRITERS_ONLY == policy) {
        unsigned (LockMgr::LockStats::*numBlockers)() const = (LockMgr::READERS_ONLY == policy)
            ? &LockMgr::LockStats::numActiveWrites
            : &LockMgr::LockStats::numActiveReads;

        while (0 < (_stats.*numBlockers)()) {
            _policyLock.wait(guard);
        }
    }
}

void LockMgr::setParent( const ResourceId& container, const ResourceId& parent ) {
    unique_lock<boost::mutex> guard(_guard);
    _containerAncestry[container] = parent;
}

void LockMgr::setTransactionPriority( const TxId& xid, int priority ) {
    unique_lock<boost::mutex> guard(_guard);
    _txPriorities[xid] = priority;
}

int LockMgr::getTransactionPriority( const TxId& xid ) {
    unique_lock<boost::mutex> guard(_guard);
    return get_transaction_priority_internal( xid );
}

LockMgr::LockId LockMgr::acquire( const TxId& requestor,
				  const unsigned& mode,
				  const ResourceId& container,
				  const ResourceId& resId,
				  Notifier* notifier) {
    unique_lock<boost::mutex> guard(_guard);

    // don't accept requests from aborted transactions
    if (_abortedTxIds.find(requestor) != _abortedTxIds.end()) {
        throw AbortException();
    }

    _stats.incRequests();

    // construct lineage from _containerAncestry
    vector<ResourceId> lineage;
    lineage.push_back( resId );
    if (container != 0) {
        lineage.push_back( container );
    }
    ResourceId nextAncestor = _containerAncestry[container];
    while (0 != nextAncestor) {
        lineage.push_back(nextAncestor);
        nextAncestor = _containerAncestry[nextAncestor];
    }

    LockId parentLock = 0;
    size_t nextAncestorIdx = lineage.size()-1;
    while (true) {
        // if modes is shorter than lineage, extend with kShared
        unsigned nextMode = LockMgr::kShared;
        if (nextAncestorIdx < sizeof(unsigned) && isExclusive(mode,nextAncestorIdx))
            nextMode = LockMgr::kExclusive;
        ResourceId container = (0==parentLock) ? 0 : _locks[parentLock]->resId;
        LockId res = acquire_internal( requestor,
                                       nextMode,
                                       container,
                                       lineage[nextAncestorIdx],
                                       notifier,
                                       guard );
        _locks[res]->parentLid = parentLock;
        parentLock = res;
        if (0 == nextAncestorIdx--) break;
    }
    isShared(mode) ? _stats.incActiveReads() : _stats.incActiveWrites();
    return parentLock;
}

LockMgr::LockId LockMgr::acquire( const TxId& requestor,
				  const std::vector<unsigned>& modes,
				  const std::vector<ResourceId>& lineage,
				  Notifier* notifier) {
    unique_lock<boost::mutex> guard(_guard);

    // don't accept requests from aborted transactions
    if (_abortedTxIds.find(requestor) != _abortedTxIds.end()) {
        throw AbortException();
    }

    _stats.incRequests();

    // loop backwards over lineage, locking ancestors first.
    LockId parentLock = 0;
    size_t nextAncestorIdx = lineage.size()-1;
    while (true) {
        // if modes is shorter than lineage, extend with kShared
        unsigned nextMode = (nextAncestorIdx < modes.size() && !modes.empty())
                          ?  modes[nextAncestorIdx] : LockMgr::kShared;
        ResourceId container = (0==parentLock) ? 0 : _locks[parentLock]->resId;
        LockId res = acquire_internal( requestor,
                                       nextMode,
                                       container,
                                       lineage[nextAncestorIdx],
                                       notifier,
                                       guard );
        _locks[res]->parentLid = parentLock;
        parentLock = res;
        if (0 == nextAncestorIdx--) break;
    }
    isShared(modes[0]) ? _stats.incActiveReads() : _stats.incActiveWrites();
    return parentLock;
}

int LockMgr::acquireOne( const TxId& requestor,
			 const unsigned& mode,
			 const ResourceId& container,
			 const vector<ResourceId>& resources,
			 Notifier* notifier) {

    if (resources.empty()) { return -1; }

    unique_lock<boost::mutex> guard(_guard);

    // don't accept requests from aborted transactions
    if (_abortedTxIds.find(requestor) != _abortedTxIds.end()) {
        throw AbortException();
    }

    _stats.incRequests();

    // construct lineage from _containerAncestry
    vector<ResourceId> lineage; // typically < 4 levels: system.database.collection.document?
    lineage.push_back( container );
    ResourceId nextAncestor = _containerAncestry[container];
    while (0 != nextAncestor) {
        lineage.push_back(nextAncestor);
        nextAncestor = _containerAncestry[nextAncestor];
    }

    // acquire locks on container hierarchy, top to bottom
    LockId parentLock = 0;
    size_t nextAncestorIdx = lineage.size()-1;
    while (true) {
        // if modes is shorter than lineage, extend with kShared
        unsigned nextMode = LockMgr::kShared;
        if (nextAncestorIdx < sizeof(unsigned) && isExclusive(mode, nextAncestorIdx+1))
            nextMode = LockMgr::kExclusive;
        ResourceId container = (0==parentLock) ? 0 : _locks[parentLock]->resId;
        LockId res = acquire_internal( requestor,
                                       nextMode,
                                       container,
                                       lineage[nextAncestorIdx],
                                       notifier,
                                       guard );
        _locks[res]->parentLid = parentLock;
        parentLock = res;
        if (0 == nextAncestorIdx--) break;
    }


    // acquire the first available recordId
    for (unsigned ix=0; ix < resources.size(); ix++) {
	if (isAvailable( requestor, mode, container, resources[ix] )) {
	    acquire_internal( requestor, mode, container, resources[ix], notifier, guard );
            isShared(mode) ? _stats.incActiveReads() : _stats.incActiveWrites();
	    return ix;
	}
    }

    // sigh. none of the records are currently available. wait on the first.
    acquire_internal( requestor, mode, container, resources[0], notifier, guard );
    isShared(mode) ? _stats.incActiveReads() : _stats.incActiveWrites();
    return 0;
}

LockMgr::LockStatus LockMgr::releaseLock( const LockId& lid ) {
    unique_lock<boost::mutex> guard(_guard);
    map<LockId,LockRequest*>::iterator it = _locks.find(lid);
    if (it != _locks.end()) {
        LockRequest* theLock = it->second;
        isShared(theLock->mode) ? _stats.decActiveReads() : _stats.decActiveWrites();
    }
    return release_internal( lid );
}

LockMgr::LockStatus LockMgr::release( const TxId& holder,
				      const unsigned& mode,
				      const ResourceId& store,
				      const ResourceId& resId) {
    unique_lock<boost::mutex> guard(_guard);

    LockId lid;
    LockMgr::LockStatus status = find_lock( holder, mode, store, resId, &lid );
    if (LockMgr::FOUND != status) {
        return status; // error, resource wasn't acquired in this mode by holder
    }
    isShared(_locks[lid]->mode) ? _stats.decActiveReads() : _stats.decActiveWrites();
    return release_internal( lid );
}

/*
 * release all resource acquired by a transaction, returning the count
 */
size_t LockMgr::release( const TxId& holder ) {
    unique_lock<boost::mutex> guard(_guard);

    map<TxId, set<LockId>*>::iterator lockIdsHeld = _xaLocks.find(holder);
    if (lockIdsHeld == _xaLocks.end()) { return 0; }
    size_t numLocksReleased = 0;
    for (set<LockId>::iterator nextLockId = lockIdsHeld->second->begin();
         nextLockId != lockIdsHeld->second->end(); ++nextLockId) {
        release_internal(*nextLockId);
        isShared(_locks[*nextLockId]->mode) ? _stats.decActiveReads() : _stats.decActiveWrites();
        numLocksReleased++;
    }
    return numLocksReleased;
}

void LockMgr::abort( const TxId& goner ) {
    unique_lock<boost::mutex> guard(_guard);
    abort_internal(goner);
}

void LockMgr::getStats( LockMgr::LockStats* out ) {
    unique_lock<boost::mutex> guard(_guard);
    *out = _stats;
}

string LockMgr::toString( ) {
    unique_lock<boost::mutex> guard(_guard);
    stringstream result;
    result << "Policy: ";
    switch(_policy) {
    case FIRST_COME:
        result << "FirstCome";
        break;
    case READERS_FIRST:
        result << "ReadersFirst";
        break;
    case OLDEST_TX_FIRST:
        result << "OldestFirst";
        break;
    case BIGGEST_BLOCKER_FIRST:
        result << "BiggestBlockerFirst";
        break;
    case READERS_ONLY:
        result << "ReadersOnly";
        break;
    case WRITERS_ONLY:
        result << "WritersOnly";
        break;
    case SHUTDOWN:
        result << "Shutdown";
        break;
    }
    result << endl;

    result << "\t_locks:" << endl;
    for (map<LockId,LockRequest*>::const_iterator locks = _locks.begin();
         locks != _locks.end(); ++locks) {
        result << "\t\t" << locks->first << locks->second->toString() << endl;
    }

    result << "\t_resourceLocks:" << endl;
    for (map<ResourceId,map<ResourceId,list<LockId>*> >::const_iterator storeLocks = _resourceLocks.begin();
         storeLocks != _resourceLocks.end(); ++storeLocks) {
        result << "\n\t\tstore=" << storeLocks->first << ": {";
        for (map<ResourceId,list<LockId>*>::const_iterator recordLocks = storeLocks->second.begin();
             recordLocks != storeLocks->second.end(); ++recordLocks) {
            bool firstResource=true;
            result << "resource=" << recordLocks->first << ": {";
            for (list<LockId>::const_iterator nextLockId = recordLocks->second->begin();
                 nextLockId != recordLocks->second->end(); ++nextLockId) {
                if (firstResource) firstResource=false;
                else result << ", ";
                result << *nextLockId;
            }
            result << "}" << endl;
        }
        result << "}" << endl;
    }

    result << "\t_waiters:" << endl;
    for (map<TxId, set<TxId>*>::const_iterator txWaiters = _waiters.begin();
         txWaiters != _waiters.end(); ++txWaiters) {
        bool firstTime=true;
        result << "\t\t" << txWaiters->first << ": {";
        for (set<TxId>::const_iterator nextWaiter = txWaiters->second->begin();
             nextWaiter != txWaiters->second->end(); ++nextWaiter) {
            if (firstTime) firstTime=false;
            else result << ", ";
            result << *nextWaiter;
        }
        result << "}" << endl;
    }

    return result.str();
}

bool LockMgr::isLocked( const TxId& holder,
                        const unsigned& mode,
                        const ResourceId& store,
                        const ResourceId& resId) {
    unique_lock<boost::mutex> guard(_guard);

    LockId unused;
    return LockMgr::FOUND == find_lock( holder, mode, store, resId, &unused );
}

/*---------- LockMgr private functions (alphabetical) ----------*/

/*
 * release resources acquired by a transaction about to abort, notifying
 * any waiters that they can retry their resource acquisition.  cleanup
 * and throw an AbortException.
 */
void LockMgr::abort_internal(const TxId& goner) {
    map<TxId, set<LockId>*>::iterator locks = _xaLocks.find(goner);

    if (locks == _xaLocks.end()) {
        // unusual, but possible to abort a transaction with no locks
        throw AbortException();
    }

    // make a copy of the TxId's locks, because releasing
    // would otherwise affect the iterator. XXX find a better way?
    //
    set<TxId> copyOfLocks = *locks->second;

    // release all resources acquired by this transaction
    // notifying any waiters that they can continue
    //
    for (set<LockId>::iterator nextLockId = copyOfLocks.begin();
         nextLockId != copyOfLocks.end(); ++nextLockId) {
        release_internal(*nextLockId);
    }

    // erase aborted transaction's waiters
    map<TxId,set<TxId>*>::iterator waiters = _waiters.find(goner);
    if (waiters != _waiters.end()) {
        delete waiters->second;
        _waiters.erase(waiters);
    }

    // add to set of aborted transactions
    _abortedTxIds.insert(goner);

    throw AbortException();
}

LockMgr::LockId LockMgr::acquire_internal( const TxId& requestor,
					   const unsigned& mode,
					   const ResourceId& store,
					   const ResourceId& resId,
					   Notifier* sleepNotifier,
                                           unique_lock<boost::mutex>& guard) {

    // handle shutdown
    if (LockMgr::SHUTDOWN == _policy &&
        (_timer.millis() > _millisToQuiesce ||
         _xaLocks.find(requestor) == _xaLocks.end())) {
        // during shutdown, don't accept requests from new transactions
        // or any requests after the quiescing period
            throw AbortException();
    }

    // if this is the 1st lock request against this store, create the entry
    if (_resourceLocks.find(store) == _resourceLocks.end()) {
        map<ResourceId, list<LockId>*> recordsLocks;
        _resourceLocks[store] = recordsLocks;
    }

    // if this is the 1st lock request against this resource, create the entry
    if (_resourceLocks[store].find(resId) == _resourceLocks[store].end()) {
        _resourceLocks[store][resId] = new list<LockId>();
    }

    // check to see if requestor has already locked resId
    list<LockId>* queue = _resourceLocks[store][resId];
    for (list<LockId>::iterator nextLockId = queue->begin();
         nextLockId != queue->end(); ++nextLockId) {
        LockRequest* nextRequest = _locks[*nextLockId];
        if (nextRequest->xid != requestor) {
            continue; // we're looking for our own locks
        }

        // requestor has some kind of lock on resId, we're either requesting
        // the same mode, a downgrade or and upgrade.

        // retrieve or create the LockRequest corresponding to this acquisition
        //
        bool hadLock = false;
        LockRequest* lr;
        if (nextRequest->mode == mode) {
            // we already have the lock, just increment the count
            lr = nextRequest;
            lr->count++;
            hadLock = true;
        }
        else {
            // create the lock request and add to TxId's set of lock requests

            // XXX should probably use placement operator new and manage LockRequest memory
            lr = new LockRequest(requestor, mode, store, resId);
            _locks[lr->lid] = lr;

            // add lock request to set of requests of requesting TxId
            map<TxId,set<LockId>*>::iterator xa_iter = _xaLocks.find(requestor);
            if (xa_iter == _xaLocks.end()) {
                set<LockId>* myLocks = new set<LockId>();
                myLocks->insert(lr->lid);
                _xaLocks[requestor] = myLocks;
            }
            else {
                xa_iter->second->insert(lr->lid);
            }
        }

        // deal with conflicts and placement of the request in the queue
        if (hadLock) {
            // requestor is active and already holds the same lock
            // the only conflict is if _policy is READERS/WRITERS_ONLY
            //
	    blockOnConflict( lr, queue, sleepNotifier, guard ); // possibly blocks
            return lr->lid;
        }
        else if (isShared(mode)) {
            // downgrade: we're asking for a kShared lock and have an EXCLUSIVE
            // since we're not blocked, the EXCLUSIVE must be at the front
            invariant(isExclusive(nextRequest->mode) && nextLockId == queue->begin());

            _stats.incDowngrades();

            // add our kShared request immediately after
            // our kExclusive request, so that when we
            // release the EXCLUSIVE, we'll be granted the SHARED
            //
            queue->insert(++nextLockId, lr->lid);

            // requestor is active and already holds the same lock
            // the only conflict is if _policy is READERS/WRITERS_ONLY
            //
	    blockOnConflict( lr, queue, sleepNotifier, guard ); // possibly blocks
            return lr->lid;
        }

        // we're asking for an upgrade.  this is ok if there are
        // no other upgrade requests (otherwise there's a deadlock)
        //
        // we expect the queue to consist of some number of read requests
        // (at least our own), followed optionally by a write request.
        //
        // if there are only read requests, our upgrade request goes to
        // the end and waits for all intervening requests to end
        //
        // if there's a write request, if it is also an upgrade, then
        // we have to abort.  Otherwise, we insert our upgrade request
        // after the last reader and before the 1st writer.

        _stats.incUpgrades();

        // gather the set of initial readLock requestor TxIds,
        // detecting deadlocks as we go
        //
        set<TxId> otherSharers; // used to detect pre-existing upgrades
        list<LockId>::iterator nextSharerId = queue->begin();
        for (; nextSharerId != queue->end(); ++nextSharerId) {
            if (*nextSharerId == nextRequest->lid) continue; // skip our own locks
            LockRequest* nextSharer = _locks[*nextSharerId];
            if (isExclusive(nextSharer->mode)) {
                set<TxId>::iterator nextUpgrader = otherSharers.find(nextSharer->xid);
                if (nextUpgrader != otherSharers.end())  {

                    // the requestor of the first exclusive lock previously requested
                    // a shared lock (it's an upgrade, so we have to abort).
                    //
                    invariant(*nextUpgrader != requestor);
                    abort_internal(requestor);
                }
                break;
            }
            otherSharers.insert(nextSharer->xid);
        }

        // safe to upgrade
        queue->insert(nextSharerId, lr->lid);
	blockOnConflict( lr, queue, sleepNotifier, guard ); // possibly blocks
        return lr->lid;
    }

    // create the lock request and add to TxId's set of lock requests
    LockRequest* lr = new LockRequest(requestor, mode, store, resId);
    _locks[lr->lid] = lr;

    // add lock request to set of requests of requesting TxId
    map<TxId,set<LockId>*>::iterator xa_iter = _xaLocks.find(requestor);
    if (xa_iter == _xaLocks.end()) {
        set<LockId>* myLocks = new set<LockId>();
        myLocks->insert(lr->lid);
        _xaLocks[requestor] = myLocks;
    }
    else {
        xa_iter->second->insert(lr->lid);
    }

    // add our request to the queue
    addLockToQueueUsingPolicy( lr );

    blockOnConflict(lr, queue, sleepNotifier, guard);
    // check for conflicts and add lr to the queue

    return lr->lid;
}

/*
 * called only when there are already LockRequests associated with a recordId
 */
void LockMgr::addLockToQueueUsingPolicy( LockMgr::LockRequest* lr ) {

    list<LockId>* queue = _resourceLocks[lr->container][lr->resId];
    list<LockId>::iterator nextLockId = queue->begin();
    if (nextLockId != queue->end()) {
        // skip over the first lock, which currently holds the lock
        // if we're incompatible with the first lock, we never want 
        // to come before it.
        //
        // if lr is shared and there are several sharers leading the queue
        // then relative position among the sharers doesn't matter
        //
        ++nextLockId;
    }

    // handle for exceptional policies
    switch(_policy) {
    case LockMgr::READERS_ONLY:
        if (isExclusive(lr->mode)) {
            // find the last reader on the queue, then advance the nextLockId
            // forward iterator past that point.
            LockId lastReader = *nextLockId;
            for (list<LockId>::reverse_iterator tail = queue->rbegin();
                 tail != queue->rend(); ++tail) {
                LockRequest* nextLock = _locks[*tail];
                if (isShared(nextLock->mode)) {
                    lastReader = nextLock->lid;
                    break;
                }
            }
            for (; nextLockId != queue->end(); ++nextLockId) {
                if (*nextLockId == lastReader) {
                    ++nextLockId;
                    break;
                }
            }
            // we're now positioned past the last reader if there is one
        }
        break;
    case LockMgr::WRITERS_ONLY:
        if (isShared(lr->mode)) {
            // find the last writer on the queue, then advance the nextLockId
            // forward iterator past that point
            LockId lastWriter = *nextLockId;
            for (list<LockId>::reverse_iterator tail = queue->rbegin();
                 tail != queue->rend(); ++tail) {
                LockRequest* nextLock = _locks[*tail];
                if (isExclusive(nextLock->mode)) {
                    lastWriter = nextLock->lid;
                    break;
                }
            }
            for (;nextLockId != queue->end(); ++nextLockId) {
                if (*nextLockId != lastWriter) {
                    ++nextLockId;
                    break;
                }
            }
            // we're now positioned past the last writer if there is one
        }
        break;
    default:
        break;
    }
    

    // use lock request's transaction's priority if specified
    int txPriority = get_transaction_priority_internal( lr->xid );
    if (txPriority > 0) {
        for (; nextLockId != queue->end(); ++nextLockId) {
            LockMgr::LockRequest* nextRequest = _locks[*nextLockId];
	    if (txPriority > get_transaction_priority_internal( nextRequest->xid )) {
                // add in front of request with lower priority that is either
		// compatible, or blocked
                queue->insert(nextLockId, lr->lid);

                // set remaining incompatible requests as lr's waiters
                addWaiters( lr, nextLockId, queue->end() );

                return;
	    }
        }
	queue->push_back( lr->lid );
	return;
    }
    else if (txPriority < 0) {
	// for now, just push to end
	// TODO: honor position of low priority requests
	queue->push_back( lr->lid );
    }

    // use LockMgr's default policy
    switch (_policy) {
    case FIRST_COME:
        queue->push_back(lr->lid);
        return;
    case READERS_FIRST:
        if (isExclusive(lr->mode)) {
            queue->push_back(lr->lid);
            return;
        }
        for (; nextLockId != queue->end(); ++nextLockId) {
            LockMgr::LockRequest* nextRequest = _locks[*nextLockId];
            if (isExclusive(nextRequest->mode) && isBlocked(nextRequest)) {
                // insert shared lock before first sleeping exclusive lock
                queue->insert(nextLockId, lr->lid);

                // set remaining incompatible requests as lr's waiters
                addWaiters( lr, nextLockId, queue->end() );
                
                return;
            }
        }
        break;
    case OLDEST_TX_FIRST:
        for (; nextLockId != queue->end(); ++nextLockId) {
            LockMgr::LockRequest* nextRequest = _locks[*nextLockId];
            if (lr->xid < nextRequest->xid &&
                (isCompatible(lr->mode, nextRequest->mode) || isBlocked(nextRequest))) {
                // smaller xid is older, so queue it before
                queue->insert(nextLockId, lr->lid);

                // set remaining incompatible requests as lr's waiters
                addWaiters( lr, nextLockId, queue->end() );
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
            if (lrNumWaiters > nextRequestNumWaiters &&
                (isCompatible(lr->mode, nextRequest->mode) || isBlocked(nextRequest))) {
                queue->insert(nextLockId, lr->lid);

                // set remaining incompatible requests as lr's waiters
                addWaiters( lr, nextLockId, queue->end() );
                return;
            }
        }
        break;
    }
    default:
        break;
    }

    queue->push_back(lr->lid);
}

void LockMgr::addWaiter( const TxId& blocker, const TxId& requestor ) {
    map<TxId, set<TxId>*>::iterator blockersWaiters = _waiters.find(blocker);
    set<TxId>* waiters;
    if (blockersWaiters == _waiters.end()) {
        waiters = new set<TxId>();
        _waiters[blocker] = waiters;
    }
    else {
        waiters = blockersWaiters->second;
    }

    waiters->insert(requestor);

    map<TxId, set<TxId>*>::iterator requestorsWaiters = _waiters.find(requestor);
    if (requestorsWaiters != _waiters.end()) {
        waiters->insert(requestorsWaiters->second->begin(),
                        requestorsWaiters->second->end());
    }
}

void LockMgr::addWaiters( LockRequest* blocker,
                          list<LockId>::iterator nextLockId,
                          list<LockId>::iterator lastLockId ) {
    for (; nextLockId != lastLockId; ++nextLockId ) {
        LockRequest* nextLockRequest = _locks[*nextLockId];
        if (! isCompatible( blocker->mode, nextLockRequest->mode)) {
            nextLockRequest->sleepCount++;
            addWaiter( blocker->xid, nextLockRequest->xid );
        }
    }
}

void LockMgr::blockOnConflict( LockRequest* lr,
			       list<LockId>* queue,
			       Notifier* sleepNotifier,
			       boost::unique_lock<boost::mutex>& guard ) {
    TxId blocker;
    if (conflictExists(lr, queue, &blocker)) {

        // call the sleep notification function once
        if (NULL != sleepNotifier) {
            // XXX should arg be xid of blocker?
            (*sleepNotifier)(lr->xid);
        }

	do {
	    // set up for future deadlock detection
	    // add requestor to blocker's waiters
	    //
	    addWaiter( blocker, lr->xid );

	    // wait for blocker to release
	    ++lr->sleepCount;
	    _stats.incBlocks();
	    while (isBlocked(lr)) {
		Timer timer;
		lr->lock.wait(guard);
		_stats.incTimeBlocked( timer.millis() );
	    }

	    // when awakened, remove ourselves from the set of waiters
	    map<TxId,set<TxId>*>::iterator blockersWaiters = _waiters.find(blocker);
	    if (blockersWaiters != _waiters.end()) {
		blockersWaiters->second->erase(lr->xid);
	    }
	} while (conflictExists(lr, queue, &blocker));
    }
}

bool LockMgr::comesBeforeUsingPolicy( const TxId& requestor,
				      const unsigned& mode,
                                      const LockMgr::LockRequest* oldRequest ) {
    if (get_transaction_priority_internal(requestor) >
        get_transaction_priority_internal(oldRequest->xid)) {
	return true;
    }

    switch (_policy) {
    case FIRST_COME:
        return false;
    case READERS_FIRST:
        return isShared(mode);
    case OLDEST_TX_FIRST:
        return requestor < oldRequest->xid;
    case BIGGEST_BLOCKER_FIRST: {
        map<TxId,set<TxId>*>::const_iterator newRequestWaiters = _waiters.find(requestor);
        if (newRequestWaiters == _waiters.end()) {
            // new request isn't blocking anything, can't come first
            return false;
        }

        map<TxId,set<TxId>*>::const_iterator oldRequestWaiters = _waiters.find(oldRequest->xid);
        if (oldRequestWaiters == _waiters.end()) {
            // old request isn't blocking anything, so new request comes first
            return true;
        }

        return newRequestWaiters->second->size() > oldRequestWaiters->second->size();
    }
    default:
        return false;
    }
}

bool LockMgr::conflictExists(const LockRequest* lr, const list<LockId>* queue, TxId* blocker) {

    // check for exceptional policies
    if (LockMgr::READERS_ONLY == _policy && isExclusive(lr->mode)) {
        *blocker = 0; // indicates blocked by LockMgr policy
        return true;
    }
    else if (LockMgr::WRITERS_ONLY == _policy && isShared(lr->mode))  {
        *blocker = 0; // indicates blocked by LockMgr policy
        return true;
    }

    set<TxId> sharedOwners;
    for (list<LockId>::const_iterator nextLockId = queue->begin();
         nextLockId != queue->end(); ++nextLockId) {

        LockRequest* nextLockRequest = _locks[*nextLockId];

        if (lr->lid == nextLockRequest->lid) {
            // if we're on the queue and haven't conflicted with anything
            // ahead of us, then there's no conflict
            return false;
        }

        if (isShared(nextLockRequest->mode)) {
            sharedOwners.insert(nextLockRequest->xid);
        }

        // no conflict if we're compatible
        if (isCompatible(lr->mode, nextLockRequest->mode)) continue;

        // no conflict if nextLock is blocked and we come before
        if (isBlocked(nextLockRequest) && comesBeforeUsingPolicy(lr->xid, lr->mode, nextLockRequest))
            continue;

        // there's a conflict
        *blocker = nextLockRequest->xid;

        // check for deadlock
        TxId requestor = lr->xid;
        map<TxId, set<TxId>*>::iterator requestorsWaiters = _waiters.find(requestor);
        if (requestorsWaiters != _waiters.end()) {
            if (requestorsWaiters->second->find(*blocker) != requestorsWaiters->second->end()) {
                // the transaction that would block requestor is already blocked by requestor
                // if requestor waited for blocker there would be a deadlock
                //
                _xaLocks[requestor]->erase(lr->lid);
                _locks.erase(lr->lid);
                delete lr;

                _stats.incDeadlocks();

                abort_internal(requestor);
            }
        }
        return true;
    }
    return false;
}

LockMgr::LockStatus LockMgr::find_lock( const TxId& holder,
                                        const unsigned& mode,
                                        const ResourceId& store,
                                        const ResourceId& resId,
                                        LockId* outLockId ) {

    *outLockId = 0; // set invalid;

    // get iterator for the resource container (store)
    map<ResourceId, map<ResourceId, list<LockId>*> >::iterator storeLocks = _resourceLocks.find(store);
    if (storeLocks == _resourceLocks.end()) { return CONTAINER_NOT_FOUND; }

    // get iterator for resId's locks
    map<ResourceId, list<LockId>*>::iterator resourceLocks = storeLocks->second.find(resId);
    if (resourceLocks == storeLocks->second.end()) { return RESOURCE_NOT_FOUND; }

    // look for an existing lock request from holder in mode
    for (list<LockId>::iterator nextLockId = resourceLocks->second->begin();
         nextLockId != resourceLocks->second->end(); ++nextLockId) {
        LockRequest* nextLockRequest = _locks[*nextLockId];
        if (nextLockRequest->xid == holder && nextLockRequest->mode == mode) {
            *outLockId = nextLockRequest->lid;
            return FOUND;
        }
    }
    return RESOURCE_NOT_FOUND_IN_MODE;
}

int LockMgr::get_transaction_priority_internal( const TxId& xid ) {
    map<TxId, int>::const_iterator txPriority = _txPriorities.find(xid);
    if (txPriority == _txPriorities.end()) {
	return 0;
    }
    return txPriority->second;
}

/*
 * Used by acquireOne
 * XXX: there's overlap between this, conflictExists and find_lock
 */
bool LockMgr::isAvailable( const TxId& requestor,
			   const unsigned& mode,
			   const ResourceId& store,
			   const ResourceId& resId ) {

    // check for exceptional policies
    if (LockMgr::READERS_ONLY == _policy && isExclusive(mode))
        return false;
    else if (LockMgr::WRITERS_ONLY == _policy && isShared(mode))
        return false;
    else if (LockMgr::SHUTDOWN == _policy &&
             (_timer.millis() > _millisToQuiesce || _xaLocks.find(requestor) == _xaLocks.end())) {
        // don't accept requests from new transaction or
        // any requests after the quiescing period
        throw AbortException();
    }

    map<ResourceId, map<ResourceId,list<LockId>*> >::iterator storeLocks = _resourceLocks.find(store);
    if (storeLocks == _resourceLocks.end()) {
	return true; // no lock requests against this container, so must be available
    }

    map<ResourceId,list<LockId>*>::iterator recLocks = storeLocks->second.find(resId);
    if (recLocks == storeLocks->second.end()) {
	return true; // no lock requests against this ResourceId, so must be available
    }

    // walk over the queue of previous requests for this ResourceId
    list<LockId>* queue = recLocks->second;
    for (list<LockId>::const_iterator nextLockId = queue->begin();
         nextLockId != queue->end(); ++nextLockId) {

        LockRequest* nextLockRequest = _locks[*nextLockId];

        if (nextLockRequest->matches(requestor, mode, store, resId)) {
            // we're already have this lock, if we're asking, we can't be asleep
	    invariant(! isBlocked(nextLockRequest));
	    return true;
        }

        // no conflict if we're compatible
        if (isCompatible(mode, nextLockRequest->mode)) continue;

        // no conflict if nextLock is blocked and we come before
        if (isBlocked(nextLockRequest) && comesBeforeUsingPolicy(requestor, mode, nextLockRequest))
            return true;

	return false; // we're incompatible and would block
    }

    // everything on the queue (if anything is on the queue) is compatible
    return true;
}

LockMgr::LockStatus LockMgr::release_internal( const LockId& lid ) {

    if (0 == lid) { return LockMgr::CONTAINER_NOT_FOUND; }

    LockRequest* lr = _locks[lid];
    const TxId holder = lr->xid;
    const unsigned mode = lr->mode;
    const ResourceId resId = lr->resId;

    ResourceId store = 0;
    if (0 != lr->parentLid) {
        LockRequest* parentReq = _locks[lr->parentLid];
        store = parentReq->resId;
    }

    map<ResourceId,map<ResourceId,list<LockId>*> >::iterator storeLocks = _resourceLocks.find(store);
    if (storeLocks == _resourceLocks.end()) {
        return CONTAINER_NOT_FOUND;
    }

    map<ResourceId,list<LockId>*>::iterator recordLocks = storeLocks->second.find(resId);
    if (recordLocks == storeLocks->second.end()) {
        return RESOURCE_NOT_FOUND;
    }

    bool seenExclusive = false;
    bool foundLock = false;
    bool foundResource = false;
    set<TxId> otherSharers;
    
    list<LockId>* queue = recordLocks->second;
    list<LockId>::iterator nextLockId = queue->begin();

    LockId parentLid = 0;

    // find the lock to release
    for( ; !foundLock && nextLockId != queue->end(); ++nextLockId) {
        LockRequest* nextLock = _locks[*nextLockId];
        if (lid != *nextLockId) {
	    if (nextLock->xid == holder) {
		foundResource = true;
	    }
            if (isShared(nextLock->mode) && !seenExclusive)
                otherSharers.insert(nextLock->xid);
            else
                seenExclusive = true;
        }
        else {
            // this is our lock.
            if (0 < --nextLock->count) { return COUNT_DECREMENTED; }

            // release the lock
            _xaLocks[holder]->erase(*nextLockId);
            _locks.erase(*nextLockId);
            queue->erase(nextLockId++);
            parentLid = nextLock->parentLid;
            delete nextLock;

            foundLock = true;
            break; // don't increment nextLockId again
        }
    }

    if (! foundLock) {
        // can't release a lock that hasn't been acquired in the specified mode
        return foundResource ? RESOURCE_NOT_FOUND_IN_MODE : RESOURCE_NOT_FOUND;
    }

    // deal with transactions that were waiting for this lock to be released
    //
    if (isExclusive(mode)) {
        bool seenSharers = false;
        for (; nextLockId != queue->end(); ++nextLockId) {
            LockRequest* nextSleeper = _locks[*nextLockId];
            if (isExclusive(nextSleeper->mode)) {
                if (!seenSharers) --nextSleeper->sleepCount;
                nextSleeper->lock.notify_one();
                break;
            }
            else {
                seenSharers = true;
                if (shouldAwake(nextSleeper)) {
                    nextSleeper->lock.notify_one();
                }
            }
        }
    }
    else if (!seenExclusive) {
        // we were one of possibly many kShared lock holders.
        // continue iterating, until we find an exclusive lock
        for (; nextLockId != queue->end(); ++nextLockId) {
            LockRequest* nextLock = _locks[*nextLockId];
            if (isShared(nextLock->mode)) {
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
                if (shouldAwake(nextLock)) {
                    nextLock->lock.notify_one();
                }
            }
            break;
        }
    }
    else {
        // we're releasing a readlock that was blocked
        // this can only happen if we're aborting
    }

    // call recursively to release ancestors' locks
    release_internal( parentLid );

    return RELEASED;
}

/*---------- ResourceLock functions ----------*/
   
ResourceLock::ResourceLock( LockMgr* lm,
			    const TxId& requestor,
			    const unsigned& mode,
			    const ResourceId& store,
			    const ResourceId& resId,
			    LockMgr::Notifier* notifier )
    : _lm(lm), _lid(0) // if acquire throws, we want this initialized
{
    _lid = lm->acquire(requestor, mode, store, resId, notifier);
}

ResourceLock::~ResourceLock( ) {
    _lm->releaseLock(_lid);
}
