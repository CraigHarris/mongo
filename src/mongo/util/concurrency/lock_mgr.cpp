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
                                   const ResourceId& resId)
    : sleep(false), parentLid(0), lid(nextLid++), xid(xid), mode(mode), resId(resId), count(1) { }

LockMgr::LockRequest::~LockRequest( ) { }

bool LockMgr::LockRequest::matches( const TxId& xid,
                                    const unsigned& mode,
                                    const LockId& parent,
                                    const ResourceId& resId ) {
    return
        this->xid == xid &&
        this->mode == mode &&
        this->parentLid == parent &&
        this->resId == resId;
}

string LockMgr::LockRequest::toString( ) const {
    stringstream result;
    result << "<lid:" << lid
           << ",parentLid: " << parentLid
           << ",xid:" << xid
           << ",mode:" << mode
           << ",resId:" << resId
           << ">";
    return result.str();
}

/*---------- Utility function ----------*/

namespace {

    bool isCompatible(const unsigned& mode1, const unsigned& mode2) {
        return mode1==mode2 && (LockMgr::kShared==mode1 || LockMgr::kShared==mode1);
    }
}

/*---------- LockMgr public functions (mutex guarded) ---------*/

unsigned const LockMgr::kShared;
unsigned const LockMgr::kExclusive;

LockMgr::LockMgr( const LockingPolicy& policy ) : _policy(policy), _guard() { }

LockMgr::~LockMgr( ) {
    unique_lock<boost::mutex> guard(_guard);
    for(map<LockId,LockRequest*>::iterator locks = _locks.begin();
        locks != _locks.end(); ++locks) {
        delete locks->second;
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
    map<TxId, int>::const_iterator txPriority = _txPriorities.find(xid);
    if (txPriority == _txPriorities.end()) {
	return 0;
    }
    return txPriority->second;
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
    vector<ResourceId> lineage(8); // typically < 4 levels: system.database.collection.document?
    lineage.push_back( resId );
    lineage.push_back( container );
    ResourceId nextAncestor = _containerAncestry[container];
    while (0 != nextAncestor) {
        lineage.push_back(nextAncestor);
        nextAncestor = _containerAncestry[nextAncestor];
    }

    LockId parent = 0;
    size_t nextAncestorIdx = lineage.size()-1;
    while (true) {
        // if modes is shorter than lineage, extend with kShared
        unsigned nextMode = (nextAncestorIdx < sizeof(unsigned))
                          ? (mode & (0x1 << nextAncestorIdx)) : LockMgr::kShared;
        parent = acquire_internal( requestor,
                                   nextMode,
                                   parent,
                                   lineage[nextAncestorIdx],
                                   notifier,
                                   guard );

        if (0 == nextAncestorIdx--) break;
    }
    return parent;
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
    LockId parent = 0;
    size_t nextAncestorIdx = lineage.size()-1;
    while (true) {
        // if modes is shorter than lineage, extend with kShared
        unsigned nextMode = (nextAncestorIdx < modes.size() && !modes.empty())
                          ?  modes[nextAncestorIdx] : LockMgr::kShared;

        parent = acquire_internal( requestor,
                                   nextMode,
                                   parent,
                                   lineage[nextAncestorIdx],
                                   notifier,
                                   guard );

        if (0 == nextAncestorIdx--) break;
    }
    return parent;
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
    vector<ResourceId> lineage(8); // typically < 4 levels: system.database.collection.document?
    lineage.push_back( container );
    ResourceId nextAncestor = _containerAncestry[container];
    while (0 != nextAncestor) {
        lineage.push_back(nextAncestor);
        nextAncestor = _containerAncestry[nextAncestor];
    }

    // acquire locks on container hierarchy, top to bottom
    LockId parent = 0;
    size_t nextAncestorIdx = lineage.size()-1;
    while (true) {
        // if modes is shorter than lineage, extend with kShared
        unsigned nextMode = (nextAncestorIdx < sizeof(unsigned))
                          ? (mode & (0x1 << (nextAncestorIdx+1))) : LockMgr::kShared;
        parent = acquire_internal( requestor,
                                   nextMode,
                                   parent,
                                   lineage[nextAncestorIdx],
                                   notifier,
                                   guard );

        if (0 == nextAncestorIdx--) break;
    }


    // acquire the first available recordId
    for (unsigned ix=0; ix < resources.size(); ix++) {
	if (isAvailable( requestor, mode, container, resources[ix] )) {
	    acquire_internal( requestor, mode, parent, resources[ix], notifier, guard );
	    return ix;
	}
    }

    // sigh. none of the records are currently available. wait on the first.
    acquire_internal( requestor, mode, parent, resources[0], notifier, guard );
    return 0;
}

LockMgr::LockStatus LockMgr::releaseLock( const LockId& lid ) {
    unique_lock<boost::mutex> guard(_guard);
    return release_internal( lid );
}

LockMgr::LockStatus LockMgr::release( const TxId& holder,
				      const unsigned& mode,
				      const ResourceId& store,
				      const ResourceId& resId) {
    unique_lock<boost::mutex> guard(_guard);

    LockId lid;
    LockMgr::LockStatus status = find_lock( holder, mode, store, resId, &lid );
    if (LockMgr::RELEASED != status && LockMgr::COUNT_DECREMENTED != status) {
        return status; // error, resource wasn't acquired in this mode by holder
    }

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
    }
    result << endl;

    result << "\t_locks:" << endl;
    for (map<LockId,LockRequest*>::const_iterator locks = _locks.begin();
         locks != _locks.end(); ++locks) {
        result << "\t\t" << locks->first << locks->second->toString() << endl;
    }

    map<ResourceId,map<ResourceId,list<LockId>*> >::const_iterator storeLocks = _resourceLocks.begin();
    result << "\t_resourceLocks:" << endl;
    for (map<ResourceId,list<LockId>*>::const_iterator recordLocks = storeLocks->second.begin();
         recordLocks != storeLocks->second.end(); ++recordLocks) {
        bool firstTime=true;
        result << "\t\t" << recordLocks->first << ": {";
        for (list<LockId>::const_iterator nextLockId = recordLocks->second->begin();
             nextLockId != recordLocks->second->end(); ++nextLockId) {
            if (firstTime) firstTime=false;
            else result << ", ";
            result << *nextLockId;
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

/*---------- LockMgr private functions ----------*/

LockMgr::LockStatus LockMgr::find_lock( const TxId& holder,
                                        const unsigned& mode,
                                        const ResourceId& store,
                                        const ResourceId& resId,
                                        LockId* outLockId ) {

    *outLockId = 0; // set invalid;

    // get iterator for the resource container (store)
    map<ResourceId, map<ResourceId, list<LockId>*> >::iterator storeLocks = _resourceLocks.begin();
    if (storeLocks == _resourceLocks.end()) { return CONTAINER_NOT_FOUND; }

    // get iterator for resId's locks
    map<ResourceId, list<LockId>*>::iterator recordLocks = storeLocks->second.begin();
    if (recordLocks == storeLocks->second.end()) { return RESOURCE_NOT_FOUND; }

    // look for an existing lock request from holder in mode
    for (list<LockId>::iterator nextLockId = recordLocks->second->begin();
         nextLockId != recordLocks->second->end(); ++nextLockId) {
        LockRequest* nextLockRequest = _locks[*nextLockId];
        if (nextLockRequest->xid == holder && nextLockRequest->mode == mode) {
            *outLockId = nextLockRequest->lid;
            return FOUND;
        }
    }
    return RESOURCE_NOT_FOUND_IN_MODE;
}

bool LockMgr::comesBeforeUsingPolicy( const TxId& requestor,
				      const unsigned& mode,
                                      const LockMgr::LockRequest* oldRequest ) {
    if (getTransactionPriority(requestor) > getTransactionPriority(oldRequest->xid)) {
	return true;
    }

    switch (_policy) {
    case FIRST_COME:
        return false;
    case READERS_FIRST:
        return kShared == mode;
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
    for(; nextLockId != lastLockId; ++nextLockId ) {
        LockRequest* nextLockRequest = _locks[*nextLockId];
        if (! isCompatible( blocker->mode, nextLockRequest->mode)) {
            addWaiter( blocker->xid, nextLockRequest->xid );
        }
    }
    
}

/*
 * called only when there are already LockRequests associated with a recordId
 */
void LockMgr::addLockToQueueUsingPolicy( LockMgr::LockRequest* lr ) {
    ResourceId container = (0 == lr->parentLid) ? 0 : _locks[lr->parentLid]->resId;
    list<LockId>* queue = _resourceLocks[container][lr->resId];
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

    // use lock request's transaction's priority if specified
    int txPriority = getTransactionPriority( lr->xid );
    if (txPriority > 0) {
        for (; nextLockId != queue->end(); ++nextLockId) {
            LockMgr::LockRequest* nextRequest = _locks[*nextLockId];
	    if (txPriority > getTransactionPriority( nextRequest->xid )) {
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
        if (LockMgr::kExclusive == lr->mode) {
            queue->push_back(lr->lid);
            return;
        }
        for (; nextLockId != queue->end(); ++nextLockId) {
            LockMgr::LockRequest* nextRequest = _locks[*nextLockId];
            if (LockMgr::kExclusive==nextRequest->mode && nextRequest->sleep) {
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
                (isCompatible(lr->mode, nextRequest->mode) || nextRequest->sleep)) {
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
                (isCompatible(lr->mode, nextRequest->mode) || nextRequest->sleep)) {
                queue->insert(nextLockId, lr->lid);

                // set remaining incompatible requests as lr's waiters
                addWaiters( lr, nextLockId, queue->end() );
                return;
            }
        }
        break;
    }
    }

    queue->push_back(lr->lid);
}

/*
 * XXX: there's overlap between this, conflictExists and find_lock
 */
bool LockMgr::isAvailable( const TxId& requestor,
			   const unsigned& mode,
			   const ResourceId& store,
			   const ResourceId& resId ) {
    
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
	    invariant( !nextLockRequest->sleep );
	    return true;
        }

        // no conflict if we're compatible
        if (isCompatible(mode, nextLockRequest->mode)) continue;

        // no conflict if nextLock is blocked and we come before
        if (nextLockRequest->sleep && comesBeforeUsingPolicy(requestor, mode, nextLockRequest)) return true;

	return false; // we're incompatible and would block
    }

    // everything on the queue (if anything is on the queue) is compatible
    return true;
}

bool LockMgr::conflictExists(const LockRequest* lr, const list<LockId>* queue, TxId* blocker) {
    set<TxId> sharedOwners;
    for (list<LockId>::const_iterator nextLockId = queue->begin();
         nextLockId != queue->end(); ++nextLockId) {

        LockRequest* nextLockRequest = _locks[*nextLockId];

        if (lr->lid == nextLockRequest->lid) {
            // if we're on the queue and haven't conflicted with anything
            // ahead of us, then there's no conflict
            return false;
        }

        if (LockMgr::kShared == nextLockRequest->mode) {
            sharedOwners.insert(nextLockRequest->xid);
        }

        // no conflict if we're compatible
        if (isCompatible(lr->mode, nextLockRequest->mode)) continue;

        // no conflict if nextLock is blocked and we come before
        if (nextLockRequest->sleep && comesBeforeUsingPolicy(lr->xid, lr->mode, nextLockRequest)) continue;

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

    // create the lock request and add to TxId's set of lock requests
    LockRequest* lr = new LockRequest(requestor, mode, resId);
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

    // if this is the 1st lock request against this store, init and exit
    map<ResourceId, map<ResourceId,list<LockId>*> >::iterator storeLocks = _resourceLocks.find(store);
    if (storeLocks == _resourceLocks.end()) {
        map<ResourceId, list<LockId>*> recordsLocks;
        list<LockId>* locks = new list<LockId>();
        locks->push_back(lr->lid);
        recordsLocks[resId] = locks;
        _resourceLocks[store] = recordsLocks;
        return lr->lid;
    }

    // if this is the 1st lock request against this resource, init and exit
    map<ResourceId,list<LockId>*>::iterator recLocks = storeLocks->second.find(resId);
    if (recLocks == storeLocks->second.end()) {
        list<LockId>* locks = new list<LockId>();
        locks->push_back(lr->lid);
        storeLocks->second.insert(pair<ResourceId,list<LockId>*>(resId,locks));
        return lr->lid;
    }

    // check to see if requestor has already locked resId
    list<LockId>* queue = recLocks->second;
    set<TxId> sharedOwners;
    for (list<LockId>::iterator nextLockId = queue->begin();
         nextLockId != queue->end(); ++nextLockId) {
        LockRequest* nextRequest = _locks[*nextLockId];
        if (nextRequest->xid != requestor) {
            if (LockMgr::kShared == mode && mode == nextRequest->mode) {
                sharedOwners.insert(nextRequest->xid);
            }
            continue;
        }

        // nextRequest is owned by requestor

        if (nextRequest->mode == mode) {
            // we already have the lock, don't need the one we created above
            _xaLocks[requestor]->erase(lr->lid);
            _locks.erase(lr->lid);
            delete lr;
            nextRequest->count++;
            return nextRequest->lid;
        }
        else if (LockMgr::kExclusive == nextRequest->mode) {
            // downgrade: we're asking for a kShared lock and have an EXCLUSIVE
            // since we're not blocked, the EXCLUSIVE must be at the front
            invariant(nextLockId == queue->begin());

            _stats.incDowngrades();

            // add our kShared request immediately after
            // our kExclusive request, so that when we
            // release the EXCLUSIVE, we'll be granted the SHARED
            //
            queue->insert(++nextLockId, lr->lid);
            return lr->lid;
        }

        // we're asking for an upgrade.  this is ok if there are
        // no other upgrade requests.
        //
        // the problem with other upgrade requests is that our view
        // of the record may change while we have a shared lock, but
        // before we're granted the exclusive lock.

        _stats.incUpgrades();

        set<TxId> otherSharers;
        list<LockId>::iterator nextSharerId = queue->begin();
        for (; nextSharerId != queue->end(); ++nextSharerId) {
            if (*nextSharerId == nextRequest->lid) continue; // skip 
            LockRequest* nextSharer = _locks[*nextSharerId];
            if (LockMgr::kExclusive == nextSharer->mode) {
                set<TxId>::iterator nextUpgrader = otherSharers.find(nextSharer->xid);
                if (nextUpgrader != otherSharers.end() && *nextUpgrader != requestor) {
                    _xaLocks[requestor]->erase(lr->lid);
                    _locks.erase(lr->lid);
                    delete lr;
                    abort_internal(requestor);
                }
                break;
            }
            otherSharers.insert(nextSharer->xid);
        }

        // safe to upgrade
        queue->insert(nextSharerId, lr->lid);
        otherSharers.erase(requestor);
        if (otherSharers.size() > 0) {
            // there are other sharers, so we block on their completion

            for (set<TxId>::iterator nextBlocker = otherSharers.begin();
                 nextBlocker != otherSharers.end(); ++nextBlocker) {
                addWaiter( *nextBlocker, requestor );
            }

            // call the sleep notification function
            if (NULL != sleepNotifier) {
                // call the sleep notification function
                // arg should be xid of blocker?
                (*sleepNotifier)(lr->xid);
            }

            // block
            lr->sleep = true;
            _stats.incBlocks();
            while (lr->sleep) {
                Timer timer;
                lr->lock.wait(guard);
                _stats.incTimeBlocked( timer.millis() );
            }

            // when awakened, remove ourselves from the set of waiters
            for (set<TxId>::iterator nextBlocker = otherSharers.begin();
                 nextBlocker != otherSharers.end(); ++nextBlocker) {
                map<TxId,set<TxId>*>::iterator nextBlockersWaiters = _waiters.find(*nextBlocker);
                if (nextBlockersWaiters != _waiters.end()) {
                    nextBlockersWaiters->second->erase(requestor);
                }
            }
        }
        return lr->lid;
    }

    // check for conflicts and add lr to the queue

    bool addedToQueue = false;
    TxId blocker;
    while (conflictExists(lr, queue, &blocker)) {
        // set up for future deadlock detection
        // add requestor to blocker's waiters
        //
        addWaiter( blocker, requestor );

        if (!addedToQueue) {
            // add our request to the queue
            addLockToQueueUsingPolicy( lr );
            addedToQueue = true;
        }

        // call the sleep notification function
        if (NULL != sleepNotifier) {
            // call the sleep notification function
            // arg should be xid of blocker?
            (*sleepNotifier)(lr->xid);
        }

        // wait for blocker to release
        lr->sleep = true;
        _stats.incBlocks();
        while (lr->sleep) {
            Timer timer;
            lr->lock.wait(guard);
            _stats.incTimeBlocked( timer.millis() );
        }

        // when awakened, remove ourselves from the set of waiters
        map<TxId,set<TxId>*>::iterator blockersWaiters = _waiters.find(blocker);
        if (blockersWaiters != _waiters.end()) {
            blockersWaiters->second->erase(requestor);
        }
    }

    if (! addedToQueue) {
        // add our request to the queue
        addLockToQueueUsingPolicy( lr );
    }

    return lr->lid;
}

LockMgr::LockStatus LockMgr::release_internal( const LockId& lid ) {
    LockRequest* lr = _locks[lid];
    const TxId& holder = lr->xid;
    const unsigned& mode = lr->mode;
    const ResourceId& resId = lr->resId;

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
    LockRequest* ourLock = NULL;

    // find the lock to release
    for( ; !foundLock && nextLockId != queue->end(); ++nextLockId) {
        LockRequest* nextLock = _locks[*nextLockId];
        if (! nextLock->matches(holder, mode, store, resId)) {
	    if (nextLock->xid == holder) {
		foundResource = true;
	    }
            if (LockMgr::kShared == nextLock->mode && !seenExclusive)
                otherSharers.insert(nextLock->xid);
            else
                seenExclusive = true;
        }
        else {
            // this is our lock.
            if (0 < --nextLock->count) { return COUNT_DECREMENTED; }

            // release the lock
            ourLock = nextLock;
            _xaLocks[holder]->erase(*nextLockId);
            _locks.erase(*nextLockId);
            queue->erase(nextLockId++); 
            delete ourLock;

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
    if (LockMgr::kExclusive == mode) {
#if 0
        // this should be true unless we've upgraded
        // perhaps we should assert that if there is a first lock on the queue
        // that it should be owned by holder?
        invariant(nextLockId == queue->begin());
#endif
        bool seenSharers = false;
        for (; nextLockId != queue->end(); ++nextLockId) {
            LockRequest* nextSleeper = _locks[*nextLockId];
            if (LockMgr::kExclusive == nextSleeper->mode) {
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
        // we were one of possibly many kShared lock holders.
        // continue iterating, until we find an exclusive lock
        for (; nextLockId != queue->end(); ++nextLockId) {
            LockRequest* nextLock = _locks[*nextLockId];
            if (LockMgr::kShared == nextLock->mode) {
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
