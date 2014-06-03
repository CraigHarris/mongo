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

LockMgr::LockStats::LockStats(const LockMgr::LockStats& other)
    : _numRequests(other._numRequests),
      _numPreexistingRequests(other._numPreexistingRequests),
      _numBlocks(other._numBlocks),
      _numDeadlocks(other._numDeadlocks),
      _numDowngrades(other._numDowngrades),
      _numUpgrades(other._numUpgrades),
      _numMillisBlocked(other._numMillisBlocked) { }

LockMgr::LockStats& LockMgr::LockStats::operator=(const LockMgr::LockStats& other) {
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

LockMgr::LockRequest::LockRequest(const TxId& xid,
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

LockMgr::LockRequest::~LockRequest() { }

bool LockMgr::LockRequest::matches(const TxId& xid,
                                   const unsigned& mode,
                                   const ResourceId& resId) {
    return
        this->xid == xid &&
        this->mode == mode &&
        this->resId == resId;
}

bool LockMgr::LockRequest::matches(const TxId& xid,
                                   const unsigned& mode,
                                   const ResourceId& container,
                                   const ResourceId& resId) {
    return
        this->xid == xid &&
        this->mode == mode &&
        this->container == container &&
        this->resId == resId;
}

string LockMgr::LockRequest::toString() const {
    stringstream result;
    result << "<lid:" << lid
           << ",parentLid: " << parentLid
           << ",xid:" << xid
           << ",mode:" << mode
           << ",resId:" << resId
           << ",container:" << container
           << ",count:" << count
           << ",sleepCount:" << sleepCount
           << ">";
    return result.str();
}

/*---------- Utility function ----------*/

namespace {

    bool isExclusive(const unsigned& mode, const unsigned level=0) {
        return 0 != (mode & (0x1 << level));
    }

    bool isShared(const unsigned& mode, const unsigned level=0) {
        return 0 == (mode & (0x1 << level));
    }

    bool isCompatible(const unsigned& mode1, const unsigned& mode2) {
        return mode1==mode2 && (isShared(mode1) || isShared(mode1));
    }

    bool isBlocked(const LockMgr::LockRequest* lr) {
        return lr->sleepCount > 0;
    }

    bool shouldAwake(LockMgr::LockRequest* lr) {
        return 0 == --lr->sleepCount;
    }

    bool hasConflict(const LockMgr::ConflictStatus& status) {
        return LockMgr::CONFLICT == status ||
               LockMgr::UPGRADE_CONFLICT == status ||
               LockMgr::POLICY_CONFLICT == status;
    }
}

/*---------- LockMgr public functions (mutex guarded) ---------*/

unsigned const LockMgr::kShared;
unsigned const LockMgr::kExclusive;
LockMgr* LockMgr::_singleton = NULL;
boost::mutex LockMgr::_getSingletonMutex;

LockMgr* LockMgr::getSingleton(const LockingPolicy& policy) {
    unique_lock<boost::mutex> guard(_getSingletonMutex);
    if (NULL == _singleton) {
        _singleton = new LockMgr(policy);
    }
    return _singleton;
}

LockMgr::LockMgr(const LockingPolicy& policy)
  : _policy(policy),
    _guard(),
    _shuttingDown(false),
    _millisToQuiesce(-1) { }

LockMgr::~LockMgr() {
    unique_lock<boost::mutex> guard(_guard);
    for(map<LockId,LockRequest*>::iterator locks = _locks.begin();
        locks != _locks.end(); ++locks) {
        delete locks->second;
    }
}

void LockMgr::shutdown(const unsigned& millisToQuiesce) {
    unique_lock<boost::mutex> guard(_guard);

#ifdef DONT_ALLOW_CHANGE_TO_QUIESCE_PERIOD
    // XXX not sure whether we want to allow multiple shutdowns
    // in order to change quiesce period?
    if (_shuttingDown) {
        return; // already in shutdown, don't extend quiescence(?)
    }
#endif

    _shuttingDown = true;
    _millisToQuiesce = millisToQuiesce;
    _timer.millisReset();
}

void LockMgr::setPolicy(const LockingPolicy& policy, Notifier* notifier) {
    unique_lock<boost::mutex> guard(_guard);
    throwIfShuttingDown();
    if (policy == _policy) return;

    LockingPolicy oldPolicy = _policy;
    _policy = policy;

    // if moving away from {READERS,WRITERS}_ONLY, awaken requests that were pending
    //
    if (READERS_ONLY == oldPolicy || WRITERS_ONLY == oldPolicy) {
        // Awaken requests that were blocked on the old policy.
        // iterate over TxIds blocked on 0 (these are blocked on policy)
        map<TxId, multiset<TxId>*>::iterator policyWaiters = _waiters.find(0);
        if (policyWaiters != _waiters.end()) {
            for (multiset<TxId>::iterator nextWaiter = policyWaiters->second->begin();
                 nextWaiter != policyWaiters->second->end(); ++nextWaiter) {
                // iterate over the locks acquired by the blocked transactions
                for (set<TxId>::iterator nextLockId = _xaLocks[*nextWaiter]->begin();
                     nextLockId != _xaLocks[*nextWaiter]->end(); ++nextLockId) {
                    LockRequest* nextLock = _locks[*nextLockId];
                    if (isBlocked(nextLock) && shouldAwake(nextLock)) {
                        // each transaction can only be blocked by one request at time
                        // this one must be due to policy that's now changed
                        nextLock->lock.notify_one();
                    }
                }
            }
            policyWaiters->second->clear();
        }
    }

    // if moving to {READERS,WRITERS}_ONLY, block until no incompatible locks
    if (READERS_ONLY == policy || WRITERS_ONLY == policy) {
        unsigned (LockMgr::LockStats::*numBlockers)() const = (READERS_ONLY == policy)
            ? &LockMgr::LockStats::numActiveWrites
            : &LockMgr::LockStats::numActiveReads;

        if (0 < (_stats.*numBlockers)()) {
            if (NULL != notifier) {
                (*notifier)(0);
            }
            do {
                _policyLock.wait(guard);
            } while (0 < (_stats.*numBlockers)());
        }
    }
}

void LockMgr::setParent(const ResourceId& container, const ResourceId& parent) {
    unique_lock<boost::mutex> guard(_guard);
    throwIfShuttingDown();
    _containerAncestry[container] = parent;
}

void LockMgr::setTransactionPriority(const TxId& xid, int priority) {
    unique_lock<boost::mutex> guard(_guard);
    throwIfShuttingDown(xid);
    _txPriorities[xid] = priority;
}

int LockMgr::getTransactionPriority(const TxId& xid) {
    unique_lock<boost::mutex> guard(_guard);
    throwIfShuttingDown(xid);
    return getTransactionPriorityInternal(xid);
}

LockMgr::LockId LockMgr::acquire(const TxId& requestor,
                                 const unsigned& mode,
                                 const ResourceId& container,
                                 const ResourceId& resId,
                                 Notifier* notifier) {
    unique_lock<boost::mutex> guard(_guard);
    throwIfShuttingDown(requestor);

    // don't accept requests from aborted transactions
    if (_abortedTxIds.find(requestor) != _abortedTxIds.end()) {
        throw AbortException();
    }

    _stats.incRequests();

    // construct lineage from _containerAncestry
    vector<ResourceId> lineage;
    lineage.push_back(resId);
    if (container != 0) {
        lineage.push_back(container);
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
        LockId res = acquireInternal(requestor, nextMode, container,
                                     lineage[nextAncestorIdx], notifier, guard);
        _locks[res]->parentLid = parentLock;
        parentLock = res;
        if (0 == nextAncestorIdx--) break;
    }
    isShared(mode) ? _stats.incActiveReads() : _stats.incActiveWrites();
    return parentLock;
}

LockMgr::LockId LockMgr::acquire(const TxId& requestor,
                                  const std::vector<unsigned>& modes,
                                  const std::vector<ResourceId>& lineage,
                                  Notifier* notifier) {
    unique_lock<boost::mutex> guard(_guard);
    throwIfShuttingDown(requestor);

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
        LockId res = acquireInternal(requestor, nextMode, container,
                                     lineage[nextAncestorIdx], notifier, guard);
        _locks[res]->parentLid = parentLock;
        parentLock = res;
        if (0 == nextAncestorIdx--) break;
    }
    isShared(modes[0]) ? _stats.incActiveReads() : _stats.incActiveWrites();
    return parentLock;
}

int LockMgr::acquireOne(const TxId& requestor,
                        const unsigned& mode,
                        const ResourceId& container,
                        const vector<ResourceId>& resources,
                        Notifier* notifier) {

    unique_lock<boost::mutex> guard(_guard);
    throwIfShuttingDown(requestor);

    if (resources.empty()) { return -1; }

    // don't accept requests from aborted transactions
    if (_abortedTxIds.find(requestor) != _abortedTxIds.end()) {
        throw AbortException();
    }

    _stats.incRequests();

    // construct lineage from _containerAncestry
    vector<ResourceId> lineage; // typically < 4 levels: system.database.collection.document?
    lineage.push_back(container);
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
        LockId res = acquireInternal(requestor, nextMode, container,
                                     lineage[nextAncestorIdx], notifier, guard);
        _locks[res]->parentLid = parentLock;
        parentLock = res;
        if (0 == nextAncestorIdx--) break;
    }


    // acquire the first available recordId
    for (unsigned ix=0; ix < resources.size(); ix++) {
        if (isAvailable(requestor, mode, container, resources[ix])) {
            acquireInternal(requestor, mode, container, resources[ix], notifier, guard);
            isShared(mode) ? _stats.incActiveReads() : _stats.incActiveWrites();
            return ix;
        }
    }

    // sigh. none of the records are currently available. wait on the first.
    acquireInternal(requestor, mode, container, resources[0], notifier, guard);
    isShared(mode) ? _stats.incActiveReads() : _stats.incActiveWrites();
    return 0;
}

LockMgr::LockStatus LockMgr::releaseLock(const LockId& lid) {
    unique_lock<boost::mutex> guard(_guard);

    map<LockId,LockRequest*>::iterator it = _locks.find(lid);
    if (it != _locks.end()) {
        LockRequest* theLock = it->second;
        throwIfShuttingDown(theLock->xid);
        isShared(theLock->mode) ? _stats.decActiveReads() : _stats.decActiveWrites();
        if ((WRITERS_ONLY == _policy && 0 == _stats.numActiveReads()) ||
            (READERS_ONLY == _policy && 0 == _stats.numActiveWrites())) {
            _policyLock.notify_one();
        }
    }
    return releaseInternal(lid);
}

LockMgr::LockStatus LockMgr::release(const TxId& holder,
                                     const unsigned& mode,
                                     const ResourceId& store,
                                     const ResourceId& resId) {
    unique_lock<boost::mutex> guard(_guard);
    throwIfShuttingDown(holder);

    LockId lid;
    LockMgr::LockStatus status = findLock(holder, mode, store, resId, &lid);
    if (LockMgr::FOUND != status) {
        return status; // error, resource wasn't acquired in this mode by holder
    }
    isShared(_locks[lid]->mode) ? _stats.decActiveReads() : _stats.decActiveWrites();
    if ((WRITERS_ONLY == _policy && 0 == _stats.numActiveReads()) ||
        (READERS_ONLY == _policy && 0 == _stats.numActiveWrites())) {
        _policyLock.notify_one();
    }
    return releaseInternal(lid);
}

/*
 * release all resource acquired by a transaction, returning the count
 */
size_t LockMgr::release(const TxId& holder) {
    unique_lock<boost::mutex> guard(_guard);
    throwIfShuttingDown(holder);

    map<TxId, set<LockId>*>::iterator lockIdsHeld = _xaLocks.find(holder);
    if (lockIdsHeld == _xaLocks.end()) { return 0; }
    size_t numLocksReleased = 0;
    for (set<LockId>::iterator nextLockId = lockIdsHeld->second->begin();
         nextLockId != lockIdsHeld->second->end(); ++nextLockId) {
        releaseInternal(*nextLockId);
        isShared(_locks[*nextLockId]->mode) ? _stats.decActiveReads() : _stats.decActiveWrites();
        if ((WRITERS_ONLY == _policy && 0 == _stats.numActiveReads()) ||
            (READERS_ONLY == _policy && 0 == _stats.numActiveWrites())) {
            _policyLock.notify_one();
        }
        numLocksReleased++;
    }
    return numLocksReleased;
}

void LockMgr::abort(const TxId& goner) {
    unique_lock<boost::mutex> guard(_guard);
    throwIfShuttingDown(goner);
    abortInternal(goner);
}

void LockMgr::getStats(LockMgr::LockStats* out) {
    unique_lock<boost::mutex> guard(_guard);
    throwIfShuttingDown();
    *out = _stats;
}

string LockMgr::toString() {
//    unique_lock<boost::mutex> guard(_guard);
#ifdef DONT_CARE_ABOUT_DEBUG_EVEN_WHEN_SHUTTING_DOWN
    // seems like we might want to allow toString for debug during shutdown?
    throwIfShuttingDown();
#endif
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
    }
    result << endl;

    if (_shuttingDown)
        result << " shutting down in " << _millisToQuiesce - _timer.millis();

    result << "\t_locks:" << endl;
    for (map<LockId,LockRequest*>::const_iterator locks = _locks.begin();
         locks != _locks.end(); ++locks) {
        result << "\t\t" << locks->first << locks->second->toString() << endl;
    }

    result << "\t_resourceLocks:" << endl;
    for (map<ResourceId,map<ResourceId,list<LockId>*> >::const_iterator storeLocks
             = _resourceLocks.begin(); storeLocks != _resourceLocks.end(); ++storeLocks) {
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
    for (map<TxId, multiset<TxId>*>::const_iterator txWaiters = _waiters.begin();
         txWaiters != _waiters.end(); ++txWaiters) {
        bool firstTime=true;
        result << "\t\t" << txWaiters->first << ": {";
        for (multiset<TxId>::const_iterator nextWaiter = txWaiters->second->begin();
             nextWaiter != txWaiters->second->end(); ++nextWaiter) {
            if (firstTime) firstTime=false;
            else result << ", ";
            result << *nextWaiter;
        }
        result << "}" << endl;
    }

    bool firstGoner = true;
    result << "\t_aborted: {" << endl;
    for (set<TxId>::iterator goners = _abortedTxIds.begin();
         goners != _abortedTxIds.end(); ++goners) {
        if (firstGoner) firstGoner = false;
        else result << ",";
        result << "t" << *goners;
    }
    result << "}";

    return result.str();
}

bool LockMgr::isLocked(const TxId& holder,
                       const unsigned& mode,
                       const ResourceId& store,
                       const ResourceId& resId) {
    unique_lock<boost::mutex> guard(_guard);
    throwIfShuttingDown(holder);

    LockId unused;
    return LockMgr::FOUND == findLock(holder, mode, store, resId, &unused);
}

/*---------- LockMgr private functions (alphabetical) ----------*/

/*
 * release resources acquired by a transaction about to abort, notifying
 * any waiters that they can retry their resource acquisition.  cleanup
 * and throw an AbortException.
 */
void LockMgr::abortInternal(const TxId& goner) {
    map<TxId, set<LockId>*>::iterator locks = _xaLocks.find(goner);

    if (locks == _xaLocks.end()) {
        // unusual, but possible to abort a transaction with no locks
        throw AbortException();
    }

    // make a copy of the TxId's locks, because releasing
    // would otherwise affect the iterator. XXX find a better way?
    //
    set<LockId> copyOfLocks = *locks->second;

    // release all resources acquired by this transaction
    // notifying any waiters that they can continue
    //
    for (set<LockId>::iterator nextLockId = copyOfLocks.begin();
         nextLockId != copyOfLocks.end(); ++nextLockId) {
        releaseInternal(*nextLockId);
    }

    // erase aborted transaction's waiters
    map<TxId,multiset<TxId>*>::iterator waiters = _waiters.find(goner);
    if (waiters != _waiters.end()) {
        delete waiters->second;
        _waiters.erase(waiters);
    }

    // add to set of aborted transactions
    _abortedTxIds.insert(goner);

    throw AbortException();
}

LockMgr::LockId LockMgr::acquireInternal(const TxId& requestor,
                                         const unsigned& mode,
                                         const ResourceId& store,
                                         const ResourceId& resId,
                                         Notifier* sleepNotifier,
                                         unique_lock<boost::mutex>& guard) {

    // if this is the 1st lock request against this store, create the entry
    if (_resourceLocks.find(store) == _resourceLocks.end()) {
        map<ResourceId, list<LockId>*> recordsLocks;
        _resourceLocks[store] = recordsLocks;
    }

    // if this is the 1st lock request against this resource, create the entry
    if (_resourceLocks[store].find(resId) == _resourceLocks[store].end()) {
        _resourceLocks[store][resId] = new list<LockId>();
    }

    list<LockId>* queue = _resourceLocks[store][resId];
    list<LockId>::iterator lastCheckedPosition = queue->begin();
    LockMgr::ConflictStatus conflictStatus = conflictExists(requestor, mode, resId,
                                                            queue, lastCheckedPosition);
    if (HAS_LOCK == conflictStatus) {
        ++_locks[*lastCheckedPosition]->count;
        return *lastCheckedPosition;
    }

    // create the lock request and add to TxId's set of lock requests

    // XXX should probably use placement operator new and manage LockRequest memory
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

    if (NO_CONFLICT == conflictStatus) {
        queue->insert(lastCheckedPosition, lr->lid);
        addWaiters(lr, lastCheckedPosition, queue->end());
        return lr->lid;
    }

    // some type of conflict
    if (UPGRADE_CONFLICT == conflictStatus) {
        queue->insert(lastCheckedPosition, lr->lid);
        addWaiters(lr, lastCheckedPosition, queue->end());
    }
    else {
        addLockToQueueUsingPolicy(lr, queue, lastCheckedPosition);
    }

    // call the sleep notification function once
    if (NULL != sleepNotifier) {
        // XXX should arg be xid of blocker?
        (*sleepNotifier)(lr->xid);
    }

    _stats.incBlocks();

    do {
        // set up for future deadlock detection add requestor to blockers' waiters
        //
        for (list<LockId>::iterator nextBlocker = queue->begin();
             nextBlocker != queue->end(); ++nextBlocker) {
            LockRequest* nextBlockingRequest = _locks[*nextBlocker];
            if (nextBlockingRequest->lid == lr->lid) {break;}
            if (nextBlockingRequest->xid == requestor) {continue;}
            if (isCompatible(_locks[*nextBlocker]->mode, lr->mode)) {continue;}
            addWaiter(_locks[*nextBlocker]->xid, requestor);
            ++lr->sleepCount;            
        }
        if (POLICY_CONFLICT == conflictStatus) {
            // to facilitate waking once the policy reverts, add requestor to system's waiters
            // where the invalid TxId 0 indicates the system
            addWaiter(0, requestor);
            ++lr->sleepCount;
        }

        // wait for blocker to release
        while (isBlocked(lr)) {
            Timer timer;
            lr->lock.wait(guard);
            _stats.incTimeBlocked(timer.millis());
        }

        lastCheckedPosition = queue->begin();
        conflictStatus = conflictExists(lr->xid, lr->mode, lr->resId, queue, lastCheckedPosition);
    } while (hasConflict(conflictStatus));

    return lr->lid;
}

/*
 * called only when there are conflicting LockRequests
 * positions a lock request (lr) in a queue at or after position
 * also adds remaining requests in queue as lr's waiters
 * for subsequent deadlock detection
 */
void LockMgr::addLockToQueueUsingPolicy(LockMgr::LockRequest* lr,
                                        list<LockId>* queue,
                                        list<LockId>::iterator& position) {

    if (position == queue->end()) {
        queue->insert(position, lr->lid);
        return;
    }
    
    // use lock request's transaction's priority if specified
    int txPriority = getTransactionPriorityInternal(lr->xid);
    if (txPriority > 0) {
        for (; position != queue->end(); ++position) {
            LockMgr::LockRequest* nextRequest = _locks[*position];
            if (txPriority > getTransactionPriorityInternal(nextRequest->xid)) {
                // add in front of request with lower priority that is either
                // compatible, or blocked
                queue->insert(position, lr->lid);

                // set remaining incompatible requests as lr's waiters
                addWaiters(lr, position, queue->end());

                return;
            }
        }
        queue->push_back(lr->lid);
        return;
    }
    else if (txPriority < 0) {
        // for now, just push to end
        // TODO: honor position of low priority requests
        queue->push_back(lr->lid);
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
        for (; position != queue->end(); ++position) {
            LockMgr::LockRequest* nextRequest = _locks[*position];
            if (isExclusive(nextRequest->mode) && isBlocked(nextRequest)) {
                // insert shared lock before first sleeping exclusive lock
                queue->insert(position, lr->lid);

                // set remaining incompatible requests as lr's waiters
                addWaiters(lr, position, queue->end());
                
                return;
            }
        }
        break;
    case OLDEST_TX_FIRST:
        for (; position != queue->end(); ++position) {
            LockMgr::LockRequest* nextRequest = _locks[*position];
            if (lr->xid < nextRequest->xid &&
                (isCompatible(lr->mode, nextRequest->mode) || isBlocked(nextRequest))) {
                // smaller xid is older, so queue it before
                queue->insert(position, lr->lid);

                // set remaining incompatible requests as lr's waiters
                addWaiters(lr, position, queue->end());
                return;
            }
        }
        break;
    case BIGGEST_BLOCKER_FIRST: {
        map<TxId,multiset<TxId>*>::iterator lrWaiters = _waiters.find(lr->xid);
        size_t lrNumWaiters = (lrWaiters == _waiters.end()) ? 0 : lrWaiters->second->size();
        for (; position != queue->end(); ++position) {
            LockMgr::LockRequest* nextRequest = _locks[*position];
            map<TxId,multiset<TxId>*>::iterator requestWaiters = _waiters.find(nextRequest->xid);
            size_t nextRequestNumWaiters =
                (requestWaiters == _waiters.end()) ? 0 : requestWaiters->second->size();
            if (lrNumWaiters > nextRequestNumWaiters &&
                (isCompatible(lr->mode, nextRequest->mode) || isBlocked(nextRequest))) {
                queue->insert(position, lr->lid);

                // set remaining incompatible requests as lr's waiters
                addWaiters(lr, position, queue->end());
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

void LockMgr::addWaiter(const TxId& blocker, const TxId& requestor) {
    if (blocker == requestor) {
        // can't wait on self
        return;
    }
    map<TxId, multiset<TxId>*>::iterator blockersWaiters = _waiters.find(blocker);
    multiset<TxId>* waiters;
    if (blockersWaiters == _waiters.end()) {
        waiters = new multiset<TxId>();
        _waiters[blocker] = waiters;
    }
    else {
        waiters = blockersWaiters->second;
    }

    waiters->insert(requestor);

    map<TxId, multiset<TxId>*>::iterator requestorsWaiters = _waiters.find(requestor);
    if (requestorsWaiters != _waiters.end()) {
        waiters->insert(requestorsWaiters->second->begin(),
                        requestorsWaiters->second->end());
    }
}

void LockMgr::addWaiters(LockRequest* blocker,
                         list<LockId>::iterator nextLockId,
                         list<LockId>::iterator lastLockId) {
    for (; nextLockId != lastLockId; ++nextLockId) {
        LockRequest* nextLockRequest = _locks[*nextLockId];
        if (! isCompatible(blocker->mode, nextLockRequest->mode)) {
            nextLockRequest->sleepCount++;
            addWaiter(blocker->xid, nextLockRequest->xid);
        }
    }
}

bool LockMgr::comesBeforeUsingPolicy(const TxId& requestor,
                                     const unsigned& mode,
                                     const LockMgr::LockRequest* oldRequest) {

    // handle special policies
    if (READERS_ONLY == _policy && kShared == mode && isBlocked(oldRequest))
        return true;
    if (WRITERS_ONLY == _policy && kExclusive == mode && isBlocked(oldRequest))
        return true;

    if (getTransactionPriorityInternal(requestor) >
        getTransactionPriorityInternal(oldRequest->xid)) {
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
        map<TxId,multiset<TxId>*>::const_iterator newReqWaiters = _waiters.find(requestor);
        if (newReqWaiters == _waiters.end()) {
            // new request isn't blocking anything, can't come first
            return false;
        }

        map<TxId,multiset<TxId>*>::const_iterator oldReqWaiters = _waiters.find(oldRequest->xid);
        if (oldReqWaiters == _waiters.end()) {
            // old request isn't blocking anything, so new request comes first
            return true;
        }

        return newReqWaiters->second->size() > oldReqWaiters->second->size();
    }
    default:
        return false;
    }
}

LockMgr::ConflictStatus LockMgr::conflictExists(const TxId& requestor,
                                                const unsigned& mode,
                                                const ResourceId& resId,
                                                list<LockId>* queue,
                                                list<LockId>::iterator& nextLockId) {

    // handle READERS/WRITERS_ONLY policy conflicts
    if ((READERS_ONLY == _policy && isExclusive(mode)) ||
        (WRITERS_ONLY == _policy && isShared(mode))) {

        if (nextLockId == queue->end()) { return POLICY_CONFLICT; }

        // position past the last active lock request on the queue
        list<LockId>::iterator lastActivePosition = queue->end();
        for (; nextLockId != queue->end(); ++nextLockId) {
            LockRequest* nextLockRequest = _locks[*nextLockId];
            if (requestor == nextLockRequest->xid && mode == nextLockRequest->mode) {
                return HAS_LOCK; // already have the lock
            }
            if (! isBlocked(nextLockRequest)) {
                lastActivePosition = nextLockId;
            }
        }
        if (lastActivePosition != queue->end()) {
            nextLockId = lastActivePosition;
        }
        return POLICY_CONFLICT;
    }
#if 0
    if (LockMgr::READERS_ONLY == _policy && isExclusive(mode)) {
        // find the last active reader on the queue, then advance the nextLockId
        // forward iterator past that point.
        //
        // 
        if (nextLockId == queue->end()) { return POLICY_CONFLICT; }
        LockId lastReader = *nextLockId;
        for (list<LockId>::reverse_iterator tail = queue->rbegin();
             tail != queue->rend(); ++tail) {
            LockRequest* nextLock = _locks[*tail];
            if (isShared(nextLock->mode) && !isBlocked(nextLock)) {
                lastReader = nextLock->lid;
                break;
            }
        }
        for (; nextLockId != queue->end(); ++nextLockId) {
            if (*nextLockId == lastReader) {
                ++nextLockId;
                break;
            }
            LockRequest* nextLockRequest = _locks[*nextLockId];
            if (requestor == nextLockRequest->xid && mode == nextLockRequest->mode)
                return HAS_LOCK; // already have the lock
        }
        return POLICY_CONFLICT;
    }
    else if (LockMgr::WRITERS_ONLY == _policy && isShared(mode))  {
        // find the last writer on the queue, then advance the nextLockId
        // forward iterator past that point
        if (nextLockId == queue->end()) { return POLICY_CONFLICT; }
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
            LockRequest* nextLockRequest = _locks[*nextLockId];
            if (requestor == nextLockRequest->xid && mode == nextLockRequest->mode)
                return HAS_LOCK; // already have the lock
        }
        return POLICY_CONFLICT;
    }
#endif
    // loop over the lock requests in the queue, looking for the 1st conflict
    // normally, we'll leave the nextLockId iterator positioned at the 1st conflict
    // if there is one, or the position (often the end) where we know there is no conflict.
    //
    // upgrades complicate this picture, because we want to position the iterator
    // after all initial share locks.  but we may not know whether an exclusived request
    // is an upgrade until we look at all the initial share locks.
    //
    // so we record the position of the 1st conflict, but continue advancing the
    // nextLockId iterator until we've seen all initial share locks.  If none have
    // the same TxId as the exclusive request, we restore the position to 1st conflict
    //
    list<LockId>::iterator firstConflict = queue->end(); // invalid
    set<TxId> sharedOwners; // all initial share lock owners
    bool alreadyHadLock = false;  // true if we see a lock with the same Txid

    for (; nextLockId != queue->end(); ++nextLockId) {

        LockRequest* nextLockRequest = _locks[*nextLockId];

        if (nextLockRequest->matches(requestor, mode, resId)) {
            // if we're already on the queue, there's no conflict
            return HAS_LOCK;
        }

        if (requestor == nextLockRequest->xid) {
            // an upgrade or downgrade request, can't conflict with ourselves
            if (isShared(mode)) {
                // downgrade
                _stats.incDowngrades();
                ++nextLockId;
                return NO_CONFLICT;
            }

            // upgrade
            alreadyHadLock = true;
            _stats.incUpgrades();
            // position after initial readers
            continue;
        }

        if (isShared(nextLockRequest->mode)) {
            invariant(!isBlocked(nextLockRequest) || WRITERS_ONLY == _policy);
            sharedOwners.insert(nextLockRequest->xid);

            if (isExclusive(mode) && firstConflict == queue->end()) {
                // if "lr" proves not to be an upgrade, restore this position later
                firstConflict = nextLockId;
            }
            // either there's no conflict yet, or we're not done checking for an upgrade
            continue;
        }

        // the next lock on the queue is an exclusive request
        invariant(isExclusive(nextLockRequest->mode));

        if (alreadyHadLock) {
            // bumped into something incompatible while up/down grading
            if (isExclusive(mode)) {
                // upgrading: bumped into another exclusive lock
                if (sharedOwners.find(nextLockRequest->xid) != sharedOwners.end()) {
                    // the exclusive lock is also an upgrade, and it must
                    // be blocked, waiting for our original share lock to be released
                    // if we wait for its shared lock, we would deadlock
                    invariant(isBlocked(nextLockRequest));
                    abortInternal(requestor);
                }

                if (sharedOwners.empty()) {
                    // simple upgrade, queue in front of nextLockRequest, no conflict
                    return NO_CONFLICT;
                }
                else {
                    // we have to wait for another shared lock before upgrading
                    return UPGRADE_CONFLICT;
                }
            }

            // downgrading, bumped into an exclusive lock, blocked on our original
            invariant (isShared(mode));
            invariant(isBlocked(nextLockRequest));
            // lr will be inserted before nextLockRequest
            return NO_CONFLICT;
        }
        else if (firstConflict != queue->end()) {
            // restore first conflict position 
            nextLockId = firstConflict;
            nextLockRequest = _locks[*nextLockId];
        }

        // no conflict if nextLock is blocked and we come before
        if (isBlocked(nextLockRequest) && comesBeforeUsingPolicy(requestor, mode, nextLockRequest)) {
            return NO_CONFLICT;
        }

        // there's a conflict, check for deadlock
        map<TxId, multiset<TxId>*>::iterator waiters = _waiters.find(requestor);
        if (waiters != _waiters.end()) {
            multiset<TxId>* requestorsWaiters = waiters->second;
            if (requestorsWaiters->find(nextLockRequest->xid) != requestorsWaiters->end()) {
                // the transaction that would block requestor is already blocked by requestor
                // if requestor waited for nextLockRequest, there would be a deadlock
                //
                _stats.incDeadlocks();

                abortInternal(requestor);
            }
        }
        return CONFLICT;
    }

    // positioned to the end of the queue
    if (alreadyHadLock && isExclusive(mode) && !sharedOwners.empty()) {
        // upgrading, queue consists of requestor's earlier share lock
        // plus other share lock.  Must wait for the others to release
        return UPGRADE_CONFLICT;
    }
    else if (firstConflict != queue->end()) {
        nextLockId = firstConflict;
        LockRequest* nextLockRequest = _locks[*nextLockId];

        if (comesBeforeUsingPolicy(requestor, mode, nextLockRequest)) {
            return NO_CONFLICT;
        }

        // there's a conflict, check for deadlock
        map<TxId, multiset<TxId>*>::iterator waiters = _waiters.find(requestor);
        if (waiters != _waiters.end()) {
            multiset<TxId>* requestorsWaiters = waiters->second;
            if (requestorsWaiters->find(nextLockRequest->xid) != requestorsWaiters->end()) {
                // the transaction that would block requestor is already blocked by requestor
                // if requestor waited for nextLockRequest, there would be a deadlock
                //
                _stats.incDeadlocks();

                abortInternal(requestor);
            }
        }
        return CONFLICT;
    }
    return NO_CONFLICT;
}

LockMgr::LockStatus LockMgr::findLock(const TxId& holder,
                                      const unsigned& mode,
                                      const ResourceId& store,
                                      const ResourceId& resId,
                                      LockId* outLockId) {

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

int LockMgr::getTransactionPriorityInternal(const TxId& xid) {
    map<TxId, int>::const_iterator txPriority = _txPriorities.find(xid);
    if (txPriority == _txPriorities.end()) {
        return 0;
    }
    return txPriority->second;
}

/*
 * Used by acquireOne
 * XXX: there's overlap between this, conflictExists and findLock
 */
bool LockMgr::isAvailable(const TxId& requestor,
                          const unsigned& mode,
                          const ResourceId& store,
                          const ResourceId& resId) {

    // check for exceptional policies
    if (READERS_ONLY == _policy && isExclusive(mode))
        return false;
    else if (WRITERS_ONLY == _policy && isShared(mode))
        return false;

    map<ResourceId, map<ResourceId,list<LockId>*> >::iterator storeLocks = _resourceLocks.find(store);
    if (storeLocks == _resourceLocks.end()) {
        return true; // no lock requests against this container, so must be available
    }

    map<ResourceId,list<LockId>*>::iterator resLocks = storeLocks->second.find(resId);
    if (resLocks == storeLocks->second.end()) {
        return true; // no lock requests against this ResourceId, so must be available
    }

    // walk over the queue of previous requests for this ResourceId
    list<LockId>* queue = resLocks->second;
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

LockMgr::LockStatus LockMgr::releaseInternal(const LockId& lid) {

    if (0 == lid) { return LockMgr::CONTAINER_NOT_FOUND; }

    LockRequest* lr = _locks[lid];
    const TxId holder = lr->xid;
    const unsigned mode = lr->mode;
    const ResourceId resId = lr->resId;
    const LockId parentLid = lr->parentLid;

    ResourceId store = 0;
    if (0 != parentLid) {
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

    bool foundLock = false;
    bool foundResource = false;
    
    list<LockId>* queue = recordLocks->second;
    list<LockId>::iterator nextLockId = queue->begin();

    // find the position of the lock to release in the queue
    for(; !foundLock && nextLockId != queue->end(); ++nextLockId) {
        LockRequest* nextLock = _locks[*nextLockId];
        if (lid != *nextLockId) {
            if (nextLock->xid == holder) {
                foundResource = true;
            }
        }
        else {
            // this is our lock.
            if (0 < --nextLock->count) { return COUNT_DECREMENTED; }

            // release the lock
            _xaLocks[holder]->erase(*nextLockId);
            _locks.erase(*nextLockId);
            queue->erase(nextLockId++);
            delete nextLock;

            foundLock = true;
            break; // don't increment nextLockId again
        }
    }

    if (! foundLock) {
        // can't release a lock that hasn't been acquired in the specified mode
        return foundResource ? RESOURCE_NOT_FOUND_IN_MODE : RESOURCE_NOT_FOUND;
    }

    if (isShared(mode)) {
        // skip over any remaining shared requests. they can't be waiting for us.
        for (; nextLockId != queue->end(); ++nextLockId) {
            LockRequest* nextLock = _locks[*nextLockId];
            if (isExclusive(nextLock->mode)) {
                break;
            }
        }
    }

    // everything left on the queue potentially conflicts with the lock just
    // released, unless it's an up/down-grade of that lock.  So iterate, and
    // when TxIds differ, decrement sleepCount, wake those with zero counts, and
    // decrement their sleep counts, waking sleepers with zero counts, and
    // cleanup state used for deadlock detection

    for (; nextLockId != queue->end(); ++nextLockId) {
        LockRequest* nextSleeper = _locks[*nextLockId];
        if (nextSleeper->xid == holder) continue;

        invariant(isBlocked(nextSleeper));
            
        // remove nextSleeper and its dependents from holder's waiters

        multiset<TxId>::iterator holdersWaiters = _waiters[holder]->find(nextSleeper->xid);
        if (holdersWaiters != _waiters[holder]->end()) {
            // every sleeper should be among holders waiters, but a previous sleeper might have 
            // had the nextSleeper as a dependent as well, in which case nextSleeer was removed 
            // previously, hence the test for finding nextSleeper among holder's waiters
            //
            _waiters[holder]->erase(holdersWaiters);
            map<TxId,multiset<TxId>*>::iterator sleepersWaiters = _waiters.find(nextSleeper->xid);
            if (sleepersWaiters != _waiters.end()) {
                for (multiset<TxId>::iterator nextSleepersWaiter = sleepersWaiters->second->begin();
                     nextSleepersWaiter != sleepersWaiters->second->end(); ++nextSleepersWaiter) {
                    _waiters[holder]->erase(*nextSleepersWaiter);
                }
            }
        }

        // wake up sleepy heads
        if (shouldAwake(nextSleeper)) {
            nextSleeper->lock.notify_one();
        }
    }

    // call recursively to release ancestors' locks
    releaseInternal(parentLid);

    return RELEASED;
}

void LockMgr::throwIfShuttingDown(const TxId& xid) const {
    if (_shuttingDown && (_timer.millis() >= _millisToQuiesce ||
                          _xaLocks.find(xid) == _xaLocks.end())) {
         
        throw AbortException(); // XXX should this be something else? ShutdownException?
    }
}

/*---------- ResourceLock functions ----------*/
   
ResourceLock::ResourceLock(LockMgr* lm,
                           const TxId& requestor,
                           const unsigned& mode,
                           const ResourceId& store,
                           const ResourceId& resId,
                           LockMgr::Notifier* notifier)
    : _lm(lm),
      _lid(0)  // if acquire throws, we want this initialized
{
    _lid = lm->acquire(requestor, mode, store, resId, notifier);
}

ResourceLock::~ResourceLock() {
    _lm->releaseLock(_lid);
}
