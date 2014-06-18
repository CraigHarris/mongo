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

#include "mongo/platform/basic.h"

#include "mongo/db/concurrency/lock_mgr.h"

#include <boost/thread/locks.hpp>
#include <sstream>

#include "mongo/util/assert_util.h"
#include "mongo/util/log.h"
#include "mongo/util/timer.h"

using boost::unique_lock;

using std::endl;
using std::exception;
using std::list;
using std::map;
using std::set;
using std::string;
using std::stringstream;
using std::vector;

namespace mongo {

    /*---------- Utility functions ----------*/

    namespace {

	/*
	 * Given a 'hierarchical' resource (e.g. document in collection in database)
	 * return true if the resource is reqeuested for exclusive access at a given level.
	 * In the example above, level 0 for document, level 1 for collection, level 2 for DB
	 * and level 3 for the system as a whole.
	 *
	 * mode is a bit vector, level is the index.
	 */
        bool isExclusive(unsigned mode, const unsigned level=0) {
            return 0 != (mode & (0x1 << level));
        }

        bool isShared(unsigned mode, const unsigned level=0) {
            return 0 == (mode & (0x1 << level));
        }

        bool isCompatible(unsigned mode1, unsigned mode2) {
            return mode1==mode2 && (isShared(mode1) || isShared(mode1));
        }

        bool hasConflict(const LockManager::ResourceStatus& status) {
            return LockManager::kResourceConflict == status ||
                LockManager::kResourceUpgradeConflict == status ||
                LockManager::kResourcePolicyConflict == status;
        }

        unsigned partitionByResource(const ResourceId& resId) {
            // when resIds are DiskLocs, their low-order bits are mostly zero
            // so add up nibbles as cheap hash
            size_t resIdValue = resId;
            size_t resIdHash = 0;
            size_t mask = 0xf;
            for (unsigned ix=0; ix < 2*sizeof(size_t); ++ix) {
                resIdHash += (resIdValue >> ix*4) & mask;
            }
            return resIdHash % kNumPartitions;
        }

        unsigned partitionByTransaction(const TxId& xid) {
            return xid % kNumPartitions;
        }
    } // namespace

    /*---------- AbortException functions ----------*/

    const char* LockManager::AbortException::what() const throw() { return "AbortException"; }

    /*---------- LockStats functions ----------*/
    string LockManager::LockStats::toString() const {
		stringstream result;
		result << "----- LockManager Stats -----" << endl
			   << "\ttotal requests: " << getNumRequests() << endl
			   << "\t# pre-existing: " << getNumPreexistingRequests() << endl
			   << "\t# same: " << getNumSameRequests() << endl
			   << "\t# times blocked: " << getNumBlocks() << endl
			   << "\t# ms blocked: " << getNumMillisBlocked() << endl
			   << "\t# deadlocks: " << getNumDeadlocks() << endl
			   << "\t# downgrades: " << getNumDowngrades() << endl
			   << "\t# upgrades: " << getNumUpgrades() << endl
			;
		return result.str();
	}

    /*---------- Transaction functions ----------*/
    Transaction::Transaction(unsigned txId, int priority)
        : _txId(txId)
        , _priority(priority)
        , _state(Transaction::kActive)
        , _locks(NULL) { }

    Transaction::~Transaction() {
        
    }

    bool Transaction::operator<(const Transaction& other) {
        return _txId < other._txId;
    }

    int Transaction::getPriority() const { return _priority; }
    void Transaction::setPriority(int newPriority) { _priority = newPriority; }

    void Transaction::removeLock(LockManager::LockRequest* lr) {
        if (lr->nextOfTransaction) {
            lr->nextOfTransaction->prevOfTransaction = lr->prevOfTransaction;
        }
        if (lr->prevOfTransaction) {
            lr->prevOfTransaction->nextOfTransaction = lr->nextOfTransaction;
        }
        else {
            _locks = lr->nextOfTransaction;
        }
        if (lr->heapAllocated) delete lr;
    }

    void Transaction::_addWaiter(Transaction* waiter) {
        _waiters.insert(waiter);
    }

    Transaction::toString() const {
        stringstream result;
        result << "<xid:" << requestor->_txId
               << ",mode:" << mode
               << ",resId:" << resId
               << ",count:" << count
               << ",sleepCount:" << sleepCount
               << ">";
        return result.str();
    }


    /*---------- LockRequest functions ----------*/


    LockManager::LockRequest::LockRequest(const ResourceId& resId,
					  const unsigned& mode,
                                          Transaction* tx,
                                          bool heapAllocated)
        : requestor(tx)
        , mode(mode)
        , resId(resId)
        , resSlice(paritionByResource(resId))
        , count(1)
        , sleepCount(0)
        , heapAllocated(heapAllocated)
        , nextOnResource(NULL)
        , prevOnResource(NULL)
        , nextOnTransaction(NULL)
        , prevOnTransaction(NULL) { }


    LockManager::LockRequest::~LockRequest() {
        verify(NULL == nextOfTransaction);
        verify(NULL == prevOfTransaction);
        verify(NULL == nextOnResource);
        verify(NULL == prevOnResource);
    }

    bool LockManager::LockRequest::matches(const Transaction* tx,
                                           const unsigned& mode,
                                           const ResourceId& resId) const {
        return
            this->requestor == tx &&
            this->mode == mode &&
            this->resId == resId;
    }

    string LockManager::LockRequest::toString() const {
        stringstream result;
        result << "<xid:" << requestor->_txId
               << ",mode:" << mode
               << ",resId:" << resId
               << ",count:" << count
               << ",sleepCount:" << sleepCount
               << ">";
        return result.str();
    }

    bool LockManager::LockRequest::isBlocked() const {
        return sleepCount > 0;
    }

    bool LockManager::LockRequest::shouldAwake() {
        return 0 == --sleepCount;
    }

    /*---------- LockManager public functions (mutex guarded) ---------*/

    unsigned const LockManager::kShared;
    unsigned const LockManager::kExclusive;

    LockManager* LockManager::_singleton = NULL;
    boost::mutex LockManager::_getSingletonMutex;
    LockManager& LockManager::getSingleton() {
        unique_lock<boost::mutex> lk(_getSingletonMutex);
        if (NULL == _singleton) {
            _singleton = new LockManager();
        }
        return *_singleton;
    }

    LockManager::LockManager(const Policy& policy)
        : _policy(policy),
          _mutex(),
          _shuttingDown(false),
          _millisToQuiesce(-1) { }

    LockManager::~LockManager() { }

    void LockManager::shutdown(const unsigned& millisToQuiesce) {
        unique_lock<boost::mutex> lk(_mutex);

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

    LockManager::Policy LockManager::getPolicy() const {
        unique_lock<boost::mutex> lk(_mutex);
        _throwIfShuttingDown();
	return _policy;
    }

    TxId LockManager::getPolicySetter() const {
        unique_lock<boost::mutex> lk(_mutex);
        _throwIfShuttingDown();
	return _policySetter;
    }
#if 0
    void LockManager::setPolicy(const TxId& xid, const Policy& policy, Notifier* notifier) {
        unique_lock<boost::mutex> lk(_mutex);
        _throwIfShuttingDown();
        if (policy == _policy) return;

        _policySetter = xid;
        Policy oldPolicy = _policy;
        _policy = policy;

        // if moving away from {READERS,WRITERS}_ONLY, awaken requests that were pending
        //
        if (kPolicyReadersOnly == oldPolicy || kPolicyWritersOnly == oldPolicy) {

            // Awaken requests that were blocked on the old policy.
            // iterate over TxIds blocked on kReservedTxId (these are blocked on policy)

            WaitersMap::iterator policyWaiters = _waiters.find(kReservedTxId);
            if (policyWaiters != _waiters.end()) {
                for (Waiters::iterator nextWaiter = policyWaiters->second.begin();
                     nextWaiter != policyWaiters->second.end(); ++nextWaiter) {

                    // iterate over the locks acquired by the blocked transactions
                    for (set<LockId>::iterator nextLockId = _xaLocks[*nextWaiter].begin();
                         nextLockId != _xaLocks[*nextWaiter].end(); ++nextLockId) {

                        LockRequest* nextLock = _locks[*nextLockId];
                        if (nextLock->isBlocked() && nextLock->shouldAwake()) {

                            // each transaction can only be blocked by one request at time
                            // this one must be due to policy that's now changed
                            nextLock->lock.notify_one();
                        }
                    }
                }
                policyWaiters->second.clear();
            }
        }

        // if moving to {READERS,WRITERS}_ONLY, block until no incompatible locks
        if (kPolicyReadersOnly == policy || kPolicyWritersOnly == policy) {
            unsigned (LockStats::*numBlockers)() const = (kPolicyReadersOnly == policy)
                ? &LockStats::numActiveWrites
                : &LockStats::numActiveReads;

            if (0 < (_stats.*numBlockers)()) {
                if (notifier) {
                    (*notifier)(kReservedTxId);
                }
                do {
                    _policyLock.wait(lk);
                } while (0 < (_stats.*numBlockers)());
            }
        }
    }
#endif
    void LockManager::acquire(LockRequest* lr, Notifier* notifier) {
        if (NULL == lr) return;
        {
            unique_lock<boost::mutex> lk(_mutex);
            _throwIfShuttingDown();
        }
        unique_lock<boost::mutex> lkRes(_resourceMutexes[lr->resSlice]);

        _acquireInternal(lr, notifier, lkRes);
    }
#if 0
    LockManager::LockId LockManager::acquire(Transaction* requestor,
                                             const uint32_t& mode,
                                             const ResourceId& resId,
                                             Notifier* notifier) {
        {
            unique_lock<boost::mutex> lk(_mutex);
            _throwIfShuttingDown();
        }
        unsigned resSlice = partitionByResource(resId);
        unsigned txSlice = partitionByTransaction(requestor);
        unique_lock<boost::mutex> lkRes(_resourceMutexes[resSlice]);
        unique_lock<boost::mutex> lkTx(_transactionMutexes[txSlice]);

        // don't accept requests from aborted transactions
        if (_abortedTxIds[txSlice].find(requestor) != _abortedTxIds[txSlice].end()) {
            throw AbortException();
        }

        return _acquireInternal(requestor, txSlice, mode, resId, resSlice, notifier, lkTx);
    }
#endif
#if 0
    int LockManager::acquireOne(const TxId& requestor,
                                const uint32_t& mode,
                                const vector<ResourceId>& resources,
                                Notifier* notifier) {

        unique_lock<boost::mutex> lk(_mutex);
        _throwIfShuttingDown(requestor);

        if (resources.empty()) { return -1; }

        // don't accept requests from aborted transactions
        if (_abortedTxIds.find(requestor) != _abortedTxIds.end()) {
            throw AbortException();
        }

        _stats.incRequests();

        // acquire the first available recordId
        for (unsigned ix=0; ix < resources.size(); ix++) {
            if (_isAvailable(requestor, mode, resources[ix])) {
                _acquireInternal(requestor, mode, resources[ix], notifier, lk);
                _stats.incStatsForMode(mode);
                return ix;
            }
        }

        // sigh. none of the records are currently available. wait on the first.
        _stats.incStatsForMode(mode);
        _acquireInternal(requestor, mode, resources[0], notifier, lk);
        return 0;
    }
#endif
    LockManager::LockStatus LockManager::releaseLock(LockRequest* lr) {
        if (NULL == lr) return kLockIdNotFound;
        {
            unique_lock<boost::mutex> lk(_mutex);
            _throwIfShuttingDown(requestor);
        }
        unique_lock<boost::mutex> lkRes(_resourceMutexes[lr->resSlice]);

        return _releaseInternal(lr);
    }
#if 0
    LockManager::LockStatus LockManager::release(const TxId& holder,
                                                 const uint32_t& mode,
                                                 const ResourceId& resId) {
        unique_lock<boost::mutex> lk(_mutex);
        _throwIfShuttingDown(holder);

        LockId lid;
        LockStatus status = _findLock(holder, mode, resId, &lid);
        if (kLockFound != status) {
            return status; // error, resource wasn't acquired in this mode by holder
        }
        _stats.decStatsForMode(_locks[lid]->mode);
        if ((kPolicyWritersOnly == _policy && 0 == _stats.numActiveReads()) ||
            (kPolicyReadersOnly == _policy && 0 == _stats.numActiveWrites())) {
            _policyLock.notify_one();
        }
        return _releaseInternal(lid);
    }
#endif
#if 0
    /*
     * release all resource acquired by a transaction, returning the count
     */
    size_t LockManager::release(Transaction* holder) {
        {
            unique_lock<boost::mutex> lk(_mutex);
            _throwIfShuttingDown(holder);
        }

        TxLockMap::iterator lockIdsHeld = _xaLocks.find(holder);
        if (lockIdsHeld == _xaLocks.end()) { return 0; }
        size_t numLocksReleased = 0;
        for (set<LockId>::iterator nextLockId = lockIdsHeld->second.begin();
             nextLockId != lockIdsHeld->second.end(); ++nextLockId) {
            _releaseInternal(*nextLockId);

            _stats.decStatsForMode(_locks[*nextLockId]->mode);

            if ((kPolicyWritersOnly == _policy && 0 == _stats.numActiveReads()) ||
                (kPolicyReadersOnly == _policy && 0 == _stats.numActiveWrites())) {
                _policyLock.notify_one();
            }
            numLocksReleased++;
        }
        return numLocksReleased;
    }
#endif
    void LockManager::abort(Transaction* goner) {
        {
            unique_lock<boost::mutex> lk(_mutex);
            _throwIfShuttingDown(goner);
        }
        _abortInternal(goner);
    }

    LockManager::LockStats LockManager::getStats() const {
        unique_lock<boost::mutex> lk(_mutex);
        _throwIfShuttingDown();
        return _stats;
    }

    string LockManager::toString() const {
//     unique_lock<boost::mutex> lk(_mutex);
#ifdef DONT_CARE_ABOUT_DEBUG_EVEN_WHEN_SHUTTING_DOWN
        // seems like we might want to allow toString for debug during shutdown?
        _throwIfShuttingDown();
#endif
        stringstream result;
        result << "Policy: ";
        switch(_policy) {
        case kPolicyFirstCome:
            result << "FirstCome";
            break;
        case kPolicyReadersFirst:
            result << "ReadersFirst";
            break;
        case kPolicyOldestTxFirst:
            result << "OldestFirst";
            break;
        case kPolicyBlockersFirst:
            result << "BiggestBlockerFirst";
            break;
        case kPolicyReadersOnly:
            result << "ReadersOnly";
            break;
        case kPolicyWritersOnly:
            result << "WritersOnly";
            break;
        }
        result << endl;

        if (_shuttingDown)
            result << " shutting down in " << _millisToQuiesce - _timer.millis();

        result << "\t_resourceLocks:" << endl;
        bool firstResource=true;
        result << "resources=" << ": {";
        for (unsigned slice=0; slice < kNumResourcePartitions; ++slice) {
            for (map<ResourceId, LockRequest>::const_iterator nextResource = _resourceLocks[slice].begin();
                 nextResource != _resourceLocks[slice].end(); ++nextResource) {
                if (firstResource) firstResource=false;
                else result << ", ";
                result << nextResource->first << ": {";
                bool firstLock=true;
                for (LockRequest* nextLock = nextResource->second;
                     nextLock; nextLock=nextLock->nextOnResource) {
                    if (firstLock) firstLock=false;
                    else result << ", ";
                    result << nextLock->toString();
                }
                result << "}";
            }
        }
        result << "}" << endl;

        result << "\tTransactions:" << endl;
        bool firstTx=true;
        for (int jx=0; jx < kNumTransactionPartitions; ++jx) {
            for (set<Transaction*>::const_iterator nextTx = _activeTransactions[jx].begin();
                 nextTx != _activeTransaction[jx].end(); ++nextTx) {
                if (firstTx) firstTx=false;
                else result << ", ";
                result << "\t\t" << nextTx->toString();
            }
        }

        return result.str();
    }

    bool LockManager::isLocked(const Transaction* holder,
                               const uint32_t& mode,
                               const ResourceId& resId) const {
        {
            unique_lock<boost::mutex> lk(_mutex);
            _throwIfShuttingDown(holder);
        }

        return kLockFound == _findLock(holder, mode, resId);
    }

    /*---------- LockManager private functions (alphabetical) ----------*/

    /*
     * release resources acquired by a transaction about to abort, notifying
     * any waiters that they can retry their resource acquisition.  cleanup
     * and throw an AbortException.
     */
    void LockManager::_abortInternal(Transaction* goner) {

        if (NULL == goner->_locks) {
            // unusual, but possible to abort a transaction with no locks
            throw AbortException();
        }

        // release all resources acquired by this transaction
        // notifying any waiters that they can continue
        //
        for (LockRequest* nextLock = goner->_locks;
             nextLock; nextLock=nextLock->nextOfTransaction) {
            _releaseInternal(nextLock);
        }

        // erase aborted transaction's waiters
        goner->_waiters.clear();

        throw AbortException();
    }

    void LockManager::_acquireInternal(LockRequest* lr,
                                       Notifier* sleepNotifier,
                                       unique_lock<boost::mutex>& guard) {

        unsigned txSlice = partitionByTransaction(lr->requestor->_txId);
        _stats[txSlice].incRequests();
        _stats[txSlice].incStatsForMode(mode);

        LockRequest* queue = _resourceLocks[lr->resSlice][resId];
        if (queue) { _stats[txSlice].incPreexisting(); }
        LockRequest* conflictPosition = queue;
        ResourceStatus resourceStatus = _conflictExists(lr->requestor, mode, resId,
                                                        queue, conflictPosition);
        if (kResourceAcquired == resourceStatus) {
            _stats[txSlice].incSame();
            ++conflictPosition->count;
            return;
        }

        // add lock request to requesting transaction's list
        lr->nextOfTransaction = lr->requestor->locks;
        lr->requestor->locks = lr;

        if (kResourceAvailable == resourceStatus) {
            if (!conflictPosition)
                queue->push_back(lr);
            else if (conflictPosition == queue) {
                lr->nextOnResource = _resourceLocks[resSlice][resId];
                _resourceLocks[resSlice][resId] = lr;
            }
            else {
                conflictPosition->prevOnResource->nextOnResource = lr;
                lr->nextOnResource = conflictPosition;
                lr->prevOnResource = conflictPosition->prevOnResource;
            }

            _addWaiters(lr, conflictPosition, NULL);
            return;
        }

        // some type of conflict, insert after confictPosition

        verify(conflictPosition);
        conflictPosition = conflictPosition->nextOnResource;

        if (kResourceUpgradeConflict == resourceStatus) {
            queue.insert(conflictPosition, lr);
        }
        else {
            _addLockToQueueUsingPolicy(lr, queue, conflictPosition);
        }
#if 0
        if (isExclusive(mode)) {
            for (LockRequest* nextFollower = conflictPosition;
                 nextFollower; nextFollower=nextFollower->nextOnResource) {
                if (nextFollower->requestor == requestor) continue;
                verify(nextFollower->isBlocked());
            }
        }
#endif
        // set remaining incompatible requests as lr's waiters
        _addWaiters(lr, conflictPosition, NULL);


        // call the sleep notification function once
        if (NULL != sleepNotifier) {
            // XXX should arg be xid of blocker?
            (*sleepNotifier)(lr->requestor->_txId);
        }

        _stats[txSlice].incBlocks();

        // this loop typically executes once
        do {
            // set up for future deadlock detection add requestor to blockers' waiters
            //
            for (LockRequest* nextBlocker = queue; nextBlocker != conflictPosition; 
                 nextBlocker=nextBlocker->nextOnResource) {
                if (nextBlocker == lr) {break;}
                if (nextBlocker->requestor == requestor) {continue;}
                if (isCompatible(nextBlocker->mode, lr->mode)) {continue;}
                _addWaiter(nextBlocker->requestor, requestor);
                ++lr->sleepCount;
            }
            if (kResourcePolicyConflict == resourceStatus) {
                // to facilitate waking once the policy reverts, add requestor to system's waiters
                _addWaiter(kReservedTxId, requestor);
                ++lr->sleepCount;
            }

            // wait for blocker to release
            while (lr->isBlocked()) {
                Timer timer;
                lr->lock.wait(guard);
                _stats.incTimeBlocked(timer.millis());
            }

            conflictPosition = queue;
            resourceStatus = _conflictExists(lr->requestor, lr->mode, lr->resId, queue, conflictPosition);
        } while (hasConflict(resourceStatus));
    }


        // create the lock request and add to TxId's set of lock requests
    }

    void LockManager::_acquireInternal(const TxId& requestor,
                                       const unsigned txSlice,
                                       const uint32_t& mode,
                                       const ResourceId& resId,
                                       const unsigned resSlice,
                                       Notifier* sleepNotifier,
                                       unique_lock<boost::mutex>& guard) {
        LockRequest* queue = _resourceLocks[resSlice][resId];
        if (!queue.empty()) { _stats[txSlice].incPreexisting(); }
        list<LockId>::iterator conflictPosition = queue.begin();
        ResourceStatus resourceStatus = _conflictExists(requestor, mode, resId,
                                                        queue, conflictPosition);
        if (kResourceAcquired == resourceStatus) {
            _stats[txSlice].incSame();
            ++_locks[resSlice][*conflictPosition]->count;
            return *conflictPosition;
        }

        // create the lock request and add to TxId's set of lock requests

        // XXX should probably use placement operator new and manage LockRequest memory
        LockRequest* lr = new LockRequest(requestor, mode, resId);
        _locks[resSlice][lr->lid] = lr;

        // add lock request to set of requests of requesting TxId
        _xaLocks[txSlice][requestor].insert(lr->lid);

        if (kResourceAvailable == resourceStatus) {
            queue.insert(conflictPosition, lr->lid);
            _addWaiters(lr, conflictPosition, queue.end());
            return lr->lid;
        }

        // some type of conflict, insert after confictPosition

        verify(conflictPosition != queue.end());
        ++conflictPosition;

        if (kResourceUpgradeConflict == resourceStatus) {
            queue.insert(conflictPosition, lr->lid);
        }
        else {
            _addLockToQueueUsingPolicy(lr, queue, conflictPosition);
        }
#if 0
        if (isExclusive(mode)) {
            for (list<LockId>::iterator followers = conflictPosition;
                 followers != queue.end(); ++followers) {
                LockRequest* nextLockRequest = _locks[resSlice][*followers];
                if (nextLockRequest->xid == requestor) continue;
                verify(nextLockRequest->isBlocked());
            }
        }
#endif
        // set remaining incompatible requests as lr's waiters
        _addWaiters(lr, conflictPosition, queue.end());


        // call the sleep notification function once
        if (NULL != sleepNotifier) {
            // XXX should arg be xid of blocker?
            (*sleepNotifier)(lr->xid);
        }

        _stats[txSlice].incBlocks();

        // this loop typically executes once
        do {
            // set up for future deadlock detection add requestor to blockers' waiters
            //
            for (list<LockId>::iterator nextBlocker = queue.begin();
                 nextBlocker != conflictPosition; ++nextBlocker) {
                LockRequest* nextBlockingRequest = _locks[resSlice][*nextBlocker];
                if (nextBlockingRequest->lid == lr->lid) {break;}
                if (nextBlockingRequest->xid == requestor) {continue;}
                if (isCompatible(_locks[resSlice][*nextBlocker]->mode, lr->mode)) {continue;}
                _addWaiter(_locks[*nextBlocker]->xid, requestor);
                ++lr->sleepCount;
            }
            if (kResourcePolicyConflict == resourceStatus) {
                // to facilitate waking once the policy reverts, add requestor to system's waiters
                _addWaiter(kReservedTxId, requestor);
                ++lr->sleepCount;
            }

            // wait for blocker to release
            while (lr->isBlocked()) {
                Timer timer;
                lr->lock.wait(guard);
                _stats.incTimeBlocked(timer.millis());
            }

            conflictPosition = queue.begin();
            resourceStatus = _conflictExists(lr->xid, lr->mode, lr->resId, queue, conflictPosition);
        } while (hasConflict(resourceStatus));

        return lr->lid;
    }

    /*
     * called only when there are conflicting LockRequests
     * positions a lock request (lr) in a queue at or after position
     * also adds remaining requests in queue as lr's waiters
     * for subsequent deadlock detection
     */
    void LockManager::_addLockToQueueUsingPolicy(LockRequest* lr,
						 LockRequest* queue,
						 LockRequest*& position) {

        if (position == NULL) {
            queue.push_back(lr->lid);
            return;
        }

        // use lock request's transaction's priority if specified
        int txPriority = lr->requestor->_getTransactionPriorityInternal();
        if (txPriority > 0) {
            for (; position; position=position->nextOnResource) {
                LockRequest* nextRequest = position;
                if (txPriority > position->requestor->_getTransactionPriorityInternal()) {
                    // add in front of request with lower priority that is either
                    // compatible, or blocked
					//
                    position.insert(lr);
                    return;
                }
            }
            queue.push_back(lr);
            return;
        }
        else if (txPriority < 0) {
            // for now, just push to end
            // TODO: honor position of low priority requests
            queue.push_back(lr);
        }

        // use LockManager's default policy
        switch (_policy) {
        case kPolicyFirstCome:
            queue.push_back(lr);
	    position = NULL;
            return;
        case kPolicyReadersFirst:
            if (isExclusive(lr->mode)) {
                queue.push_back(lr);
                position = NULL;
                return;
            }
            for (; position; position=position->nextOnResource) {
                if (isExclusive(position->mode) && position->isBlocked()) {
                    // insert shared lock before first sleeping exclusive lock
                    position.insert(lr);
                    return;
                }
            }
            break;
        case kPolicyOldestTxFirst:
            for (; position; position=position->nextOnResource) {
                if (lr->requestor < position->requestor &&
                    (isCompatible(lr->mode, position->mode) || position->isBlocked())) {
                    // smaller xid is older, so queue it before
                    position.insert(lr->lid);
                    return;
                }
            }
            break;
        case kPolicyBlockersFirst: {
            size_t lrNumWaiters = lr->requestor->_waiters.size();
            for (; position; position=position->nextOnResource) {
                size_t nextRequestNumWaiters = position->requestor->_waiters.size();
                if (lrNumWaiters > nextRequestNumWaiters &&
                    (isCompatible(lr->mode, position->mode) || position->isBlocked())) {
                    position.insert(lr);
                    return;
                }
            }
            break;
        }
        default:
            break;
        }

        queue.push_back(lr);
	position = NULL;
    }

    void LockManager::_addWaiter(const TxId& blocker, const TxId& requestor) {
        if (blocker == requestor) {
            // can't wait on self
            return;
        }

		// add requestor to blocker's waiters
		_waiters[blocker].insert(requestor);

		// add all of requestor's waiters to blocker's waiters
		_waiters[blocker].insert(_waiters[requestor].begin(), _waiters[requestor].end());
    }

    void LockManager::_addWaiters(LockRequest* blocker,
				  LockRequest* nextLock,
				  LockRequest* lastLock) {
        for (; nextLock != lastLock; nextLock=nextLock->nextOnResource) {
            if (! isCompatible(blocker->mode, nextLock->mode)) {
                if (nextLock->sleepCount > 0) {
		    _addWaiter(blocker->requestor, nextLockRequest->requestor);
		    ++nextLock->sleepCount;
		}
            }
        }
    }

    bool LockManager::_comesBeforeUsingPolicy(const Transaction* requestor,
					      const unsigned& mode,
					      const LockRequest* oldRequest) const {

        // handle special policies
        if (kPolicyReadersOnly == _policy && kShared == mode && oldRequest->isBlocked())
            return true;
        if (kPolicyWritersOnly == _policy && kExclusive == mode && oldRequest->isBlocked())
            return true;

        if (requestor->_getTransactionPriorityInternal() >
            oldRequest->requestor->_getTransactionPriorityInternal()) {
            return true;
        }

        switch (_policy) {
        case kPolicyFirstCome:
            return false;
        case kPolicyReadersFirst:
            return isShared(mode);
        case kPolicyOldestTxFirst:
            return requestor < oldRequest->requestor;
        case kPolicyBlockersFirst: {
            return requestor->_waiters.size() > oldRequest->requestor->_waiters.size();
        }
        default:
            return false;
        }
    }

    LockManager::ResourceStatus LockManager::_conflictExists(const Transaction* requestor,
                                                             const unsigned& mode,
                                                             const ResourceId& resId,
                                                             LockRequest* queue,
                                                             LockRequest*& nextLock) {

        // handle READERS/kPolicyWritersOnly policy conflicts
        if ((kPolicyReadersOnly == _policy && isExclusive(mode)) ||
            (kPolicyWritersOnly == _policy && isShared(mode))) {

            if (NULL == nextLock) { return kResourcePolicyConflict; }

            // position past the last active lock request on the queue
            LockRequest* lastActivePosition = NULL;
            for (; nextLock; nextLock = nextLock->nextOnResource) {
                if (requestor == nextLock->requestor && mode == nextLock->mode) {
                    return kResourceAcquired; // already have the lock
                }
                if (! nextLock->isBlocked()) {
                    lastActivePosition = nextLock;
                }
            }
            if (lastActivePosition) {
                nextLock = lastActivePosition;
            }
            return kResourcePolicyConflict;
        }

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
        LockRequest* firstConflict = NULL;
        set<Transaction*> sharedOwners; // all initial share lock owners
        bool alreadyHadLock = false;  // true if we see a lock with the same Txid

        for (; nextLock; nextLock=nextLock->nextOnResource) {

            if (nextLock->matches(requestor, mode, resId)) {
                // if we're already on the queue, there's no conflict
                return kResourceAcquired;
            }

            if (requestor == nextLock->requestor) {
                // an upgrade or downgrade request, can't conflict with ourselves
                if (isShared(mode)) {
                    // downgrade
                    _stats[txSlice].incDowngrades();
                    nextLock = nextLock->nextOnResource;
                    return kResourceAvailable;
                }

                // upgrade
                alreadyHadLock = true;
                _stats[txSlice].incUpgrades();
                // position after initial readers
                continue;
            }

            if (isShared(nextLock->mode)) {
                invariant(!nextLock->isBlocked() || kPolicyWritersOnly == _policy);

                sharedOwners.insert(nextLock->requestor);

                if (isExclusive(mode) && firstConflict == NULL) {
                    // if "lr" proves not to be an upgrade, restore this position later
                    firstConflict = nextLock;
                }
                // either there's no conflict yet, or we're not done checking for an upgrade
                continue;
            }

            // the next lock on the queue is an exclusive request
            invariant(isExclusive(nextLock->mode));

            if (alreadyHadLock) {
                // bumped into something incompatible while up/down grading
                if (isExclusive(mode)) {
                    // upgrading: bumped into another exclusive lock
                    if (sharedOwners.find(nextLock->requestor) != sharedOwners.end()) {
                        // the exclusive lock is also an upgrade, and it must
                        // be blocked, waiting for our original share lock to be released
                        // if we wait for its shared lock, we would deadlock
                        //
                        invariant(nextLock->isBlocked());
                        _abortInternal(requestor);
                    }

                    if (sharedOwners.empty()) {
                        // simple upgrade, queue in front of nextLockRequest, no conflict
                        return kResourceAvailable;
                    }
                    else {
                        // we have to wait for another shared lock before upgrading
                        return kResourceUpgradeConflict;
                    }
                }

                // downgrading, bumped into an exclusive lock, blocked on our original
                invariant (isShared(mode));
                invariant(nextLock->isBlocked());
                // lr will be inserted before nextLockRequest
                return kResourceAvailable;
            }
            else if (firstConflict) {
                // restore first conflict position
                nextLock = firstConflict;
            }

            // no conflict if nextLock is blocked and we come before
            if (nextLock->isBlocked() &&
                _comesBeforeUsingPolicy(requestor, mode, nextLock)) {
                return kResourceAvailable;
            }

            // there's a conflict, check for deadlock
            if (requestor->_waiters.find(nextLock->requestor) != requestor->_waiters.end()) {
                // the transaction that would block requestor is already blocked by requestor
                // if requestor waited for nextLockRequest, there would be a deadlock
                //
                _stats[txSlice].incDeadlocks();
                _abortInternal(requestor);
            }
            return kResourceConflict;
        }

        // positioned to the end of the queue
        if (alreadyHadLock && isExclusive(mode) && !sharedOwners.empty()) {
            // upgrading, queue consists of requestor's earlier share lock
            // plus other share lock.  Must wait for the others to release
            return kResourceUpgradeConflict;
        }
        else if (firstConflict) {
            nextLock = firstConflict;

            if (_comesBeforeUsingPolicy(requestor, mode, nextLock)) {
                return kResourceAvailable;
            }

            // there's a conflict, check for deadlock
            WaitersMap::iterator waiters = _waiters.find(requestor);
            if (requestor->_waiters.find(nextLock->requestor) != requestor->_waiters.end()) {
                // the transaction that would block requestor is already blocked by requestor
                // if requestor waited for nextLockRequest, there would be a deadlock
                //
                _stats[txSlice].incDeadlocks();
                _abortInternal(requestor);
            }
            return kResourceConflict;
        }
        return kResourceAvailable;
    }

    LockManager::LockStatus LockManager::_findLock(const Transaction* holder,
                                                   const unsigned& mode,
                                                   const ResourceId& resId,
                                                   unsigned resSlice,
                                                   LockRequest*& outLock) const {

        *outLockId = kReservedLockId; // set invalid;

        // get iterator for resId's locks
        LockRequest* resourceLocks = _resourceLocks[resSlice].find(resId);
        if (resourceLocks == _resourceLocks[resSlice].end()) { return kLockResourceNotFound; }

        // look for an existing lock request from holder in mode
        for (LockRequest* nextLock = resourceLocks->second;
             nextLock; nextLock=nextLock->nextOnResource) {
            if (nextLock->requestor == holder && nextLock->mode == mode) {
                *outLock = nextLock;
                return kLockFound;
            }
        }
        return kLockModeNotFound;
    }

    /*
     * Used by acquireOne
     * XXX: there's overlap between this, _conflictExists and _findLock
     */
    bool LockManager::_isAvailable(const TxId& requestor,
                                   const unsigned& mode,
                                   const ResourceId& resId) const {

        // check for exceptional policies
        if (kPolicyReadersOnly == _policy && isExclusive(mode))
            return false;
        else if (kPolicyWritersOnly == _policy && isShared(mode))
            return false;

        ResourceLocks::const_iterator resLocks = _resourceLocks.find(resId);
        if (resLocks == _resourceLocks.end()) {
            return true; // no lock requests against this ResourceId, so must be available
        }

        // walk over the queue of previous requests for this ResourceId
        const list<LockId>& queue = resLocks->second;
        for (list<LockId>::const_iterator nextLockId = queue.begin();
             nextLockId != queue.end(); ++nextLockId) {

            LockRequest* nextLockRequest = _locks.at(*nextLockId);

            if (nextLockRequest->matches(requestor, mode, resId)) {
                // we're already have this lock, if we're asking, we can't be asleep
                invariant(! nextLockRequest->isBlocked());
                return true;
            }

            // no conflict if we're compatible
            if (isCompatible(mode, nextLockRequest->mode)) continue;

            // no conflict if nextLock is blocked and we come before
            if (nextLockRequest->isBlocked() && _comesBeforeUsingPolicy(requestor, mode, nextLockRequest))
                return true;

            return false; // we're incompatible and would block
        }

        // everything on the queue (if anything is on the queue) is compatible
        return true;
    }

    LockManager::LockStatus LockManager::_releaseInternal(const LockRequest* lr, unsigned resSlice) {
        const Transaction* holder = lr->requestor;
        const unsigned mode = lr->mode;
        const ResourceId resId = lr->resId;

        _stats.decStatsForMode(theLock->mode);
        if ((kPolicyWritersOnly == _policy && 0 == _stats.numActiveReads()) ||
            (kPolicyReadersOnly == _policy && 0 == _stats.numActiveWrites())) {
            _policyLock.notify_one();
        }

        LockRequest* queue = _resourceLocks[resSlice].find(resId);
        if (NULL == queue) {
            return kLockResourceNotFound;
        }

        bool foundLock = false;
        bool foundResource = false;

        LockRequest* nextLock = queue;

        // find the position of the lock to release in the queue
        for(; !foundLock && nextLock; nextLock=nextLock->nextOnResource) {
            if (lr != nextLock) {
                if (nextLock->requestor == holder) {
                    foundResource = true;
                }
            }
            else {
                // this is our lock.
                if (0 < --nextLock->count) { return kLockCountDecremented; }

                // release the lock
                removeFromResourceQueue(nextLock);
                holder->removeLock(nextLock);

                foundLock = true;
                break; // don't increment nextLockId again
            }
        }

        if (! foundLock) {
            // can't release a lock that hasn't been acquired in the specified mode
            return foundResource ? kLockModeNotFound : kLockResourceNotFound;
        }

        if (isShared(mode)) {
            // skip over any remaining shared requests. they can't be waiting for us.
            for (; nextLock; nextLock=nextLock->nextOnResource) {
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

        for (; nextLock; nextLock=nextLock->nextOnResource) {
            LockRequest* nextSleeper = nextLock;
            if (nextSleeper->requestor == holder) continue;

            invariant(nextSleeper->isBlocked());

            // remove nextSleeper and its dependents from holder's waiters

            if (holder->_waiters.find(nextSleeper->requestor) != holder->_waiters.end()) {
                // every sleeper should be among holders waiters, but a previous sleeper might have
                // had the nextSleeper as a dependent as well, in which case nextSleeper was removed
                // previously, hence the test for finding nextSleeper among holder's waiters
                //
                holder->_waiters.erase(nextSleeper->requestor);
                for (set<Transaction*>::iterator nextSleepersWaiter = nextSleeper->_waiters.begin();
                         nextSleepersWaiter != nextSleeper->_waiters.end(); ++nextSleepersWaiter) {
                        holder->_waiters.erase(*nextSleepersWaiter);
                    }
                }
            }

            // wake up sleepy heads
            if (nextSleeper->shouldAwake()) {
                nextSleeper->lock.notify_one();
            }
        }
#if 0
        // verify stuff
        if (holder->_waiters.empty()) {
            for (set<Transaction*>::iterator nextTx = _activeTransactions.begin();
                 nextTx != _activeTransactions.end(); ++nextTx) {
                verify( (*nextTx)->_waiters.find(holder) == (*nextTx)->_waiters.end());
            }
        }

	if (queue) {
	    verify(!queue->isBlocked());
	}
#endif
        return kLockReleased;
    }

    void LockManager::_throwIfShuttingDown(const TxId& xid, unsigned txSlice) const {
        if (_shuttingDown && (_timer.millis() >= _millisToQuiesce ||
                              _xaLocks[txSlice].find(xid) == _xaLocks[txSlice].end())) {

            throw AbortException(); // XXX should this be something else? ShutdownException?
        }
    }

    /*---------- ResourceLock functions ----------*/

    ResourceLock::ResourceLock(LockManager& lm,
                               Transaction* requestor,
                               const uint32_t& mode,
                               const ResourceId& resId,
                               LockManager::Notifier* notifier)
        : _lm(lm)
        , _lr(resId, mode, requestor)
    {
        _lm.acquire(&_lr, notifier);
    }

    ResourceLock::~ResourceLock() {
        _lm.releaseLock(_lid);
    }

} // namespace mongo
