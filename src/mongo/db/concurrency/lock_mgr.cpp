/*
 *
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

#include "mongo/db/concurrency/lock_mode.h"
#include "mongo/db/concurrency/lock_request.h"
#include "mongo/db/concurrency/resource_id.h"
#include "mongo/db/concurrency/transaction.h"

#include <boost/thread/locks.hpp>
#include <sstream>

#include "mongo/base/init.h"
#include "mongo/db/server_parameters.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/log.h"
#include "mongo/util/timer.h"

using std::endl;
using std::exception;
using std::map;
using std::multiset;
using std::set;
using std::string;
using std::stringstream;

namespace mongo {

    using namespace Locking;

    // This parameter enables experimental document-level locking features
    // It should be removed once full document-level locking is checked-in.
    MONGO_EXPORT_SERVER_PARAMETER(useExperimentalDocLocking, bool, false);

    static LockManager* _singleton = NULL;

    MONGO_INITIALIZER(InstantiateLockManager)(InitializerContext* context) {
        _singleton = new LockManager();
        return Status::OK();
    }

    LockManager& LockManager::getSingleton() {
        return *_singleton;
    }

    LockManager::LockManager(const Policy& policy)
        : _policy(policy)
        , _mutex()
        , _shuttingDown(false)
        , _millisToQuiesce(-1)
        , _systemTransaction(new Transaction(0))
        , _numCurrentActiveReadRequests(0)
        , _numCurrentActiveWriteRequests(0)
    { }

    LockManager::~LockManager() {
        delete _systemTransaction;
    }

    void LockManager::shutdown(const unsigned& millisToQuiesce) {
        if (!useExperimentalDocLocking) return;

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
        boost::unique_lock<boost::mutex> lk(_mutex);
        _throwIfShuttingDown();
        return _policy;
    }

    void LockManager::setPolicy(Transaction* tx, const Policy& policy, Notifier* notifier) {
        if (!useExperimentalDocLocking) return;

        _throwIfShuttingDown();

        boost::unique_lock<boost::mutex> lk(_mutex);
        
        if (policy == _policy) return;

        _policySetter = tx;
        Policy oldPolicy = _policy;
        _policy = policy;

        // if moving away from {READERS,WRITERS}_ONLY, awaken requests that were pending
        //
        if (kPolicyReadersOnly == oldPolicy || kPolicyWritersOnly == oldPolicy) {

            // Awaken requests that were blocked on the old policy.
            // iterate over TxIds blocked on kReservedTxId (these are blocked on policy)

            for (multiset<const Transaction*>::iterator nextWaiter = _systemTransaction->_waiters.begin();
                 nextWaiter != _systemTransaction->_waiters.end(); ++nextWaiter) {

                // iterate over the locks acquired by the blocked transactions
                for (LockRequest* nextLock = (*nextWaiter)->_locks; nextLock;
                     nextLock = nextLock->nextOfTransaction) {
                    if (nextLock->isBlocked() && nextLock->shouldAwake()) {

                        // each transaction can only be blocked by one request at time
                        // this one must be due to policy that's now changed
                        nextLock->requestor->wake();
                    }
                }
            }
            _systemTransaction->removeAllWaiters();
        }

        // if moving to {READERS,WRITERS}_ONLY, block until no incompatible locks
        if (kPolicyReadersOnly == policy || kPolicyWritersOnly == policy) {
            unsigned (LockManager::*numBlockers)() const = (kPolicyReadersOnly == policy)
                ? &LockManager::_numActiveWrites
                : &LockManager::_numActiveReads;

            if ((this->*numBlockers)() > 0) {
                if (notifier) {
                    (*notifier)(_systemTransaction);
                }
                do {
                    _policyLock.wait(lk);
                } while ((this->*numBlockers)() > 0);
            }
        }
    }

    void LockManager::acquireLock(LockRequest* lr, Notifier* notifier) {
        if (!useExperimentalDocLocking) return;

        _throwIfShuttingDown();

        invariant(lr);

        boost::unique_lock<boost::mutex> lk(_resourceMutexes[lr->slice]);

        LockRequest* queue = _findQueue(lr->slice, lr->resId);
        LockRequest* conflictPosition = queue;
        ResourceStatus status = _getConflictInfo(lr->requestor, lr->mode, lr->resId, lr->slice,
                                                 queue, conflictPosition);
        if (kResourceAcquired == status) { return; }

        // add lock request to requesting transaction's list
        lr->requestor->addLock(lr);

        _acquireInternal(lr, queue, conflictPosition, status, notifier, lk);
        _incStatsForMode(lr->mode);
    }

    void LockManager::acquireLockUnderParent(LockRequest* lr,
                                             const ResourceId& parentId,
                                             Notifier* notifier) {
        if (!useExperimentalDocLocking) return;

        _throwIfShuttingDown();

        invariant(lr);

        {
            // check that parentId is locked, and return if in same mode as child
            unsigned parentSlice = partitionResource(parentId);
            boost::unique_lock<boost::mutex> lk(_resourceMutexes[parentSlice]);
            LockRequest* parentLock;
            LockStatus status = _findLock(lr->requestor, lr->mode, parentId, parentSlice, parentLock);
            if (kLockFound == status) return;
            invariant(kLockModeNotFound == status); // parent must be locked in different mode
        }

        boost::unique_lock<boost::mutex> lk(_resourceMutexes[lr->slice]);

        LockRequest* queue = _findQueue(lr->slice, lr->resId);
        LockRequest* conflictPosition = queue;
        ResourceStatus status = _getConflictInfo(lr->requestor, lr->mode, lr->resId, lr->slice,
                                                 queue, conflictPosition);
        if (kResourceAcquired == status) { return; }

        // add lock request to requesting transaction's list
        lr->requestor->addLock(lr);

        _acquireInternal(lr, queue, conflictPosition, status, notifier, lk);
        _incStatsForMode(lr->mode);
    }

    void LockManager::acquire(Transaction* requestor,
                              const LockMode& mode,
                              const ResourceId& resId,
                              Notifier* notifier) {
        if (kReservedResourceId == resId || !useExperimentalDocLocking) return;

        _throwIfShuttingDown();

        unsigned slice = partitionResource(resId);
        boost::unique_lock<boost::mutex> lk(_resourceMutexes[slice]);

        LockRequest* queue = _findQueue(slice, resId);
        LockRequest* conflictPosition = queue;
        ResourceStatus status = _getConflictInfo(requestor, mode, resId, slice,
                                                 queue, conflictPosition);
        if (kResourceAcquired == status) { return; }

        LockRequest* lr = new LockRequest(resId, mode, requestor, true);

        // add lock request to requesting transaction's list
        lr->requestor->addLock(lr);

        _acquireInternal(lr, queue, conflictPosition, status, notifier, lk);
        _incStatsForMode(mode);
    }

    void LockManager::acquireUnderParent(Transaction* requestor,
                                         const LockMode& mode,
                                         const ResourceId& resId,
                                         const ResourceId& parentId,
                                         Notifier* notifier) {
        if (kReservedResourceId == resId || !useExperimentalDocLocking) return;

        _throwIfShuttingDown();

        {
            // check that parentId is locked, and return if in same mode as child
            unsigned parentSlice = partitionResource(parentId);
            boost::unique_lock<boost::mutex> lk(_resourceMutexes[parentSlice]);
            LockRequest* parentLock;
            LockStatus status = _findLock(requestor, mode, parentId, parentSlice, parentLock);
            if (kLockFound == status) return;
            invariant(kLockModeNotFound == status); // parent must be locked in different mode
        }

        unsigned slice = partitionResource(resId);
        boost::unique_lock<boost::mutex> lk(_resourceMutexes[slice]);

        LockRequest* queue = _findQueue(slice, resId);
        LockRequest* conflictPosition = queue;
        ResourceStatus status = _getConflictInfo(requestor, mode, resId, slice,
                                                 queue, conflictPosition);
        if (kResourceAcquired == status) { return; }

        LockRequest* lr = new LockRequest(resId, mode, requestor, true);

        // add lock request to requesting transaction's list
        lr->requestor->addLock(lr);

        _acquireInternal(lr, queue, conflictPosition, status, notifier, lk);
        _incStatsForMode(mode);
    }

    LockManager::LockStatus LockManager::releaseLock(LockRequest* lr) {
        if (!useExperimentalDocLocking) return kLockNotFound;
        invariant(lr);
        boost::unique_lock<boost::mutex> lk(_resourceMutexes[lr->slice]);
        _decStatsForMode(lr->mode);
        return _releaseInternal(lr);
    }

    LockManager::LockStatus LockManager::release(const Transaction* holder,
                                                 const LockMode& mode,
                                                 const ResourceId& resId) {
        if (kReservedResourceId == resId || !useExperimentalDocLocking) return kLockNotFound;

        unsigned slice = partitionResource(resId);
        boost::unique_lock<boost::mutex> lk(_resourceMutexes[slice]);

        LockRequest* lr;
        LockStatus status = _findLock(holder, mode, resId, slice, lr);
        if (kLockFound != status) {
            return status; // error, resource wasn't acquired in this mode by holder
        }
        _decStatsForMode(mode);
        return _releaseInternal(lr);
    }

    /*
     * release all resource acquired by a transaction, returning the count
     */
    void LockManager::relinquishScopedTxLocks(LockRequest* locks) {
        if (!useExperimentalDocLocking) return;

        if (NULL == locks) return;

        LockRequest* nextLock = locks;
        while (nextLock) {
            if (!nextLock->acquiredInScope) {
                nextLock = nextLock->nextOfTransaction;
                continue;
            }

            invariant(0 == nextLock->count);

            // _releaseInternal may free nextLock
            LockRequest* newNextLock = nextLock->nextOfTransaction;
            boost::unique_lock<boost::mutex> lk(_resourceMutexes[nextLock->slice]);
            _releaseInternal(nextLock);
            nextLock = newNextLock;
        }
    }

    LockManager::LockStats LockManager::getStats() const {
        boost::unique_lock<boost::mutex> lk(_mutex);
        _throwIfShuttingDown();

        LockStats result;
        for (unsigned ix=0; ix < kNumResourcePartitions; ix++) {
            result += _stats[ix];
        }
        return result;
    }

    string LockManager::toString() const {
        // don't acquire lock on mutex, better to have corrupt data than no data

        stringstream result;
        result << "Policy: ";
        switch(_policy) {
        case kPolicyFirstCome:
            result << "FirstCome";
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
            result << "slice[" << slice << "]:";
            for (map<ResourceId, LockRequest*>::const_iterator nextResource = _resourceLocks[slice].begin();
                 nextResource != _resourceLocks[slice].end(); ++nextResource) {
                if (firstResource) firstResource=false;
                else result << ", ";
                result << nextResource->first.toString() << ": {";
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
#ifdef REGISTER_TRANSACTIONS
        result << "\tTransactions:" << endl;
        bool firstTx=true;
        for (unsigned jx=0; jx < kNumTransactionPartitions; ++jx) {
            for (set<Transaction*>::const_iterator nextTx = _activeTransactions[jx].begin();
                 nextTx != _activeTransactions[jx].end(); ++nextTx) {
                if (firstTx) firstTx=false;
                else result << ", ";
                result << "\t\t" << (*nextTx)->toString();
            }
        }
#endif
        return result.str();
    }

    bool LockManager::isLocked(const Transaction* holder,
                               const LockMode& mode,
                               const ResourceId& resId) const {
        if (!useExperimentalDocLocking) return false;

        _throwIfShuttingDown(holder);

        LockRequest* theLock=NULL;
        return kLockFound == _findLock(holder, mode, resId, partitionResource(resId), theLock);
    }

    unsigned LockManager::partitionResource(const ResourceId& resId) {
        return resId.hash() % kNumResourcePartitions;
    }

#ifdef REGISTER_TRANSACTIONS
    unsigned LockManager::partitionTransaction(unsigned xid) {
        return xid % kNumTransactionPartitions;
    }
#endif

    void LockManager::_push_back(LockRequest* lr) {
        LockRequest* nextLock = _findQueue(lr->slice, lr->resId);
        if (NULL == nextLock) {
            _resourceLocks[lr->slice][lr->resId] = lr;
            return;
        }

        while (nextLock->nextOnResource) {
            nextLock = nextLock->nextOnResource;
        }

        nextLock->append(lr);
    }

    void LockManager::_removeFromResourceQueue(LockRequest* lr) {
        if (lr->nextOnResource) {
            lr->nextOnResource->prevOnResource = lr->prevOnResource;
        }
        if (lr->prevOnResource) {
            lr->prevOnResource->nextOnResource = lr->nextOnResource;
        }
        else if (NULL == lr->nextOnResource) {
            _resourceLocks[lr->slice].erase(lr->resId);
        }
        else {
            _resourceLocks[lr->slice][lr->resId] = lr->nextOnResource;
        }
        lr->nextOnResource = NULL;
        lr->prevOnResource = NULL;
    }

    /*---------- LockManager private functions (alphabetical) ----------*/

    LockManager::ResourceStatus LockManager::_getConflictInfo(Transaction* requestor,
                                                              const LockMode& mode,
                                                              const ResourceId& resId,
                                                              unsigned slice,
                                                              LockRequest* queue,
                                                              LockRequest*& conflictPosition) {
        _stats[slice].incRequests();

        if (queue) { _stats[slice].incPreexisting(); }

        ResourceStatus resourceStatus = _conflictExists(requestor, mode, resId,
                                                        slice, conflictPosition);

        if (kResourceAcquired == resourceStatus) {
            _stats[slice].incSame();
            ++conflictPosition->count;
        }
        return resourceStatus;
    }

    void LockManager::_acquireInternal(LockRequest* lr,
                                       LockRequest* queue,
                                       LockRequest* conflictPosition,
                                       ResourceStatus resourceStatus,
                                       Notifier* sleepNotifier,
                                       boost::unique_lock<boost::mutex>& guard) {

        if (kResourceAvailable == resourceStatus) {
            if (!conflictPosition)
                _push_back(lr);
            else if (conflictPosition == queue) {
                lr->nextOnResource = conflictPosition;
                conflictPosition->prevOnResource = lr;
                _resourceLocks[lr->slice][lr->resId] = lr;
            }
            else {
                conflictPosition->insert(lr);
            }

            _addWaiters(lr, conflictPosition, NULL);
            return;
        }

        // some type of conflict, insert after confictPosition

        verify(conflictPosition ||
               kResourcePolicyConflict == resourceStatus ||
               kResourceUpgradeConflict == resourceStatus);

        if (conflictPosition) {
            conflictPosition = conflictPosition->nextOnResource;
        }

        if (kResourceUpgradeConflict == resourceStatus) {
            if (conflictPosition)
                conflictPosition->insert(lr);
            else
                _push_back(lr);
        }
        else {
            _addLockToQueueUsingPolicy(lr, queue, conflictPosition);
        }

        // set remaining incompatible requests as lr's waiters
        _addWaiters(lr, conflictPosition, NULL);


        // call the sleep notification function once
        if (NULL != sleepNotifier) {
            // XXX should arg be xid of blocker?
            (*sleepNotifier)(lr->requestor);
        }

        _stats[lr->slice].incBlocks();

        // set up for future deadlock detection add requestor to blockers' waiters
        //
        for (LockRequest* nextBlocker = queue; nextBlocker != conflictPosition; 
             nextBlocker=nextBlocker->nextOnResource) {
            if (nextBlocker == lr) {break;}
            if (nextBlocker->requestor == lr->requestor) {continue;}
            if (isCompatible(nextBlocker->mode, lr->mode)) {continue;}
            nextBlocker->requestor->addWaiter(lr->requestor);
            ++lr->sleepCount;
        }
        if (kResourcePolicyConflict == resourceStatus) {
            // to facilitate waking once the policy reverts, add requestor to system's waiters
            _systemTransaction->addWaiter(lr->requestor);
            ++lr->sleepCount;
        }

        // wait for blocker to release
        while (lr->isBlocked()) {
            Timer timer;
            lr->requestor->wait(guard);
            _stats[lr->slice].incTimeBlocked(timer.millis());
        }
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
            _push_back(lr);
            return;
        }

        // use lock request's transaction's priority if specified
        int txPriority = lr->requestor->getPriority();
        if (txPriority > 0) {
            for (; position; position=position->nextOnResource) {
                if (txPriority > position->requestor->getPriority()) {
                    // add in front of request with lower priority that is either
                    // compatible, or blocked
                    //
                    position->insert(lr);
                    return;
                }
            }
            _push_back(lr);
            return;
        }
        else if (txPriority < 0) {
            // for now, just push to end
            // TODO: honor position of low priority requests
            _push_back(lr);
        }

        // use LockManager's default policy
        switch (_policy) {
        case kPolicyFirstCome:
            _push_back(lr);
            position = NULL;
            return;
        case kPolicyOldestTxFirst:
            for (; position; position=position->nextOnResource) {
                if (*lr->requestor < *position->requestor &&
                    (isCompatible(lr->mode, position->mode) || position->isBlocked())) {
                    // smaller xid is older, so queue it before
                    position->insert(lr);
                    return;
                }
            }
            break;
        case kPolicyBlockersFirst: {
            size_t lrNumWaiters = lr->requestor->numWaiters();
            for (; position; position=position->nextOnResource) {
                size_t nextRequestNumWaiters = position->requestor->numWaiters();
                if (lrNumWaiters > nextRequestNumWaiters &&
                    (isCompatible(position->mode,lr->mode) || position->isBlocked())) {
                    position->insert(lr);
                    return;
                }
            }
            break;
        }
        default:
            break;
        }

        _push_back(lr);
        position = NULL;
    }

    void LockManager::_addWaiters(LockRequest* blocker,
                                  LockRequest* nextLock,
                                  LockRequest* lastLock) {
        for (; nextLock != lastLock; nextLock=nextLock->nextOnResource) {
            if (! isCompatible(blocker->mode, nextLock->mode)) {
                if (nextLock->sleepCount > 0) {
                    blocker->requestor->addWaiter(nextLock->requestor);
                    ++nextLock->sleepCount;
                }
            }
        }
    }

    bool LockManager::_comesBeforeUsingPolicy(const Transaction* requestor,
                                              const LockMode& mode,
                                              const LockRequest* oldRequest) const {

        // handle special policies
        if (kPolicyReadersOnly == _policy && kShared == mode && oldRequest->isBlocked())
            return true;
        if (kPolicyWritersOnly == _policy && kExclusive == mode && oldRequest->isBlocked())
            return true;

        if (requestor->getPriority() >
            oldRequest->requestor->getPriority()) {
            return true;
        }

        switch (_policy) {
        case kPolicyFirstCome:
            return false;
        case kPolicyOldestTxFirst:
            return *requestor < *oldRequest->requestor;
        case kPolicyBlockersFirst: {
            return requestor->numWaiters() > oldRequest->requestor->numWaiters();
        }
        default:
            return false;
        }
    }

    LockManager::ResourceStatus LockManager::_conflictExists(Transaction* requestor,
                                                             const LockMode& mode,
                                                             const ResourceId& resId,
                                                             unsigned slice,
                                                             LockRequest*& nextLock) {

        // handle READERS/kPolicyWritersOnly policy conflicts
        if ((kPolicyReadersOnly == _policy && conflictsWithReadersOnlyPolicy(mode)) ||
            (kPolicyWritersOnly == _policy && conflictsWithWritersOnlyPolicy(mode))) {

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
        // normally, we'll leave the nextLock positioned at the 1st conflict
        // if there is one, or the position (often the end) where we know that
        // there is no conflict.
        //
        // upgrades complicate this picture. we want to position the upgrade request
        // after the last active incompatible lock, not at the 1st incompatible lock.
        // but we dont know if we have an upgrade until we look at the initial locks.
        //
        // so we remember the position of the 1st conflict, but continue advancing
        // until we've seen all active incompatible locks.  If none of those have
        // the same Transaction as the request, we restore the position to 1st conflict

        LockRequest* firstConflict = NULL;
        set<Transaction*> activeOwners; // initial active owners. if has requestor, it's an upgrade
        bool alreadyHadLock = false;  // true if we see a lock with the same Txid

        for (; nextLock && nextLock->isActive(); nextLock=nextLock->nextOnResource) {

            if (requestor == nextLock->requestor) {

                if (nextLock->matches(requestor, mode, resId)) {
                    // we've already locked the resource in the requested mode
                    return kResourceAcquired;
                }

                if (isDowngrade(nextLock->mode, mode)) {
                    _stats[slice].incDowngrades();
                    nextLock = nextLock->nextOnResource;
                    return kResourceAvailable;
                }

                if (isUpgrade(nextLock->mode, mode)) {
                    if (!alreadyHadLock) {
                        alreadyHadLock = true;
                        _stats[slice].incUpgrades();
                    }
                }

                // there are new lock acquisitions that are neither downgrades, nor
                // upgrades.  For example, holding kShared and requesting kIntentExclusive
                // But we never conflict with ourselves.

                continue;
            }

            // next lock owner != requestor

            activeOwners.insert(nextLock->requestor);

            if (NULL == firstConflict && !isCompatible(nextLock->mode, mode)) {
                // if we're not an upgrade request, restore this position later
                firstConflict = nextLock;
            }
        }

        if (alreadyHadLock) {
            // upgrading

            if (nextLock && activeOwners.find(nextLock->requestor) != activeOwners.end()) {
                // nextLock is also an upgrade
                requestor->abort();
            }

            if (NULL == firstConflict)
                return kResourceAvailable;

            return kResourceUpgradeConflict;
        }
        else if (firstConflict) {
            // restore first conflict position
            nextLock = firstConflict;
        }

        if (nextLock) {

            // no conflict if nextLock is blocked and we come before
            if (nextLock->isBlocked() &&
                _comesBeforeUsingPolicy(requestor, mode, nextLock)) {
                return kResourceAvailable;
            }

            // there's a conflict, check for deadlock
            if (requestor->hasWaiter(nextLock->requestor)) {
                // the transaction that would block requestor is already blocked by requestor
                // if requestor waited for nextLockRequest, there would be a deadlock
                //
                _stats[slice].incDeadlocks();
                requestor->abort();
            }
            return kResourceConflict;
        }

        return kResourceAvailable;
    }

    LockManager::LockStatus LockManager::_findLock(const Transaction* holder,
                                                   const LockMode& mode,
                                                   const ResourceId& resId,
                                                   unsigned slice,
                                                   LockRequest*& outLock) const {

        outLock = NULL; // set invalid;

        // get iterator for resId's locks
        map<ResourceId,LockRequest*>::const_iterator resLocks = _resourceLocks[slice].find(resId);
        if (resLocks == _resourceLocks[slice].end()) { return kLockResourceNotFound; }

        // look for an existing lock request from holder in mode
        bool holderFound = false;
        for (LockRequest* nextLock = resLocks->second;
             nextLock; nextLock=nextLock->nextOnResource) {
            if (nextLock->requestor == holder) {
                if (nextLock->mode == mode) {
                    outLock = nextLock;
                    return kLockFound;
                }
                else {
                    holderFound = true;
                }
            }
        }
        return holderFound ? kLockModeNotFound : kLockResourceNotFound;
    }

    LockRequest* LockManager::_findQueue(unsigned slice, const ResourceId& resId) const {
        map<ResourceId,LockRequest*>::const_iterator it = _resourceLocks[slice].find(resId);
        if (it == _resourceLocks[slice].end()) { return NULL; }
        return it->second;
    }

    /**
     *  normally decrement lr's lock count, and if zero, wake up sleepers and delete lr
     */
    LockManager::LockStatus LockManager::_releaseInternal(LockRequest* lr) {

        if (lr->count > 0) {
            if (--lr->count > 0) {
                return kLockCountDecremented;
            }
        }
        else {
            invariant(lr->acquiredInScope && !lr->requestor->inScope());
        }

        if (lr->requestor->inScope()) {
            // delay waking sleepers and cleanup until requestor is not inScope
            return kLockFound;
        }

        if ((kPolicyWritersOnly == _policy && 0 == _numActiveReads()) ||
            (kPolicyReadersOnly == _policy && 0 == _numActiveWrites())) {
            _policyLock.notify_one();
        }

        Transaction* holder = lr->requestor;
        LockRequest* nextLock = lr->nextOnResource;

        // remove the lock from its queues
        _removeFromResourceQueue(lr);
        holder->removeLock(lr);

        // find the sleepers waiting for lr.  They:
        //
        //    * are blocked requests of other transactions, that
        //    * follow lr on the queue, and
        //    * are incompatible with lr's mode.
        //
        // decrement their sleep counts, waking sleepers with zero counts, and
        // cleanup state used for deadlock detection

        for (; nextLock; nextLock=nextLock->nextOnResource) {
            if (nextLock->isActive()) continue;

            if (nextLock->requestor == holder) continue;

            // remove nextSleeper and its dependents from holder's waiters
            holder->removeWaiterAndItsWaiters(nextLock->requestor);

            // wake up sleepy heads
            if (nextLock->shouldAwake()) {
                nextLock->requestor->wake();
            }
        }

        return kLockReleased;
    }

    void LockManager::_throwIfShuttingDown(const Transaction* tx) const {

        if (_shuttingDown && (_timer.millis() >= _millisToQuiesce)) {
            throw Transaction::AbortException(); // XXX should this be something else? ShutdownException?
        }
    }

} // namespace mongo
