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

#pragma once

#include <boost/thread/condition_variable.hpp>
#include <boost/thread/mutex.hpp>
#include <iterator>
#include <map>
#include <set>
#include <string>

#include "mongo/platform/compiler.h"
#include "mongo/platform/cstdint.h"
#include "mongo/platform/atomic_word.h"
#include "mongo/base/string_data.h"
#include "mongo/util/timer.h"

#include "mongo/db/concurrency/lock_mode.h"
#include "mongo/db/concurrency/resource_id.h"

/*
 * LockManager controls access to resources through two functions: acquire and release
 *
 * Resources can be databases, collections, oplogs, records, btree-nodes, key-value pairs,
 * forward/backward pointers, or anything at all that can be unambiguously identified.
 *
 * Resources are acquired by Transactions for some use described by a LockMode.
 * A table defines which lock modes are compatible or conflict with other modes. If an
 * acquisition request conflicts with a pre-existing use of a resource, the requesting
 * transaction will block until the original conflicting requests and any new conflicting
 * requests have been released.
 *
 * Contention for a resource is resolved by a LockingPolicy, which determines which blocked
 * resource requests to awaken when the blocker releases the resource.
 *
 */

namespace mongo {

    // Defined in lock_mgr.cpp
    extern bool useExperimentalDocLocking;

    class LockRequest;
    class Transaction;

    class LockManager {
    public:

        /**
         * Used to decide which blocked requests to honor first when resource becomes available
         */
        enum Policy {
            kPolicyFirstCome,     // wake the first blocked request in arrival order
            kPolicyOldestTxFirst, // wake the blocked request with the lowest TxId
            kPolicyBlockersFirst, // wake the blocked request which is itself the most blocking
            kPolicyReadersOnly,   // block write requests (used during fsync)
            kPolicyWritersOnly    // block read requests
        };

        /**
         * returned by ::conflictExists, called from acquire
         */
        enum ResourceStatus {
            kResourceAcquired,       // requested resource was already acquired, increment count
            kResourceAvailable,      // requested resource is available. no waiting
            kResourceConflict,       // requested resource is in use by another transaction
            kResourcePolicyConflict, // requested mode blocked by READER/kPolicyWritersOnly policy
            kResourceUpgradeConflict // requested resource was previously acquired for shared use
                                     // now requested for exclusive use, but there are other shared
                                     // users, so this request must wait.
        };

        /**
         * returned by find_lock(), and release() and, mostly for testing
         * explains why a lock wasn't found or released
         */
        enum LockStatus {
            kLockFound,             // found a matching lock request
            kLockReleased,          // released requested lock
            kLockCountDecremented,  // decremented lock count, but didn't release
            kLockNotFound,          // specific lock not found
            kLockResourceNotFound,  // no locks on the resource
            kLockModeNotFound       // locks on the resource, but not of the specified mode
        };


        /**
         * A Notifier is used to do something just before an acquisition request blocks.
         *
         * XXX: should perhaps define Notifier as a functor so C++11 lambda's match
         * for the test.cpp, it was convenient for the call operator to access private
         * state, which is why we're using the class formulation for now
         */
        class Notifier {
        public:
            virtual ~Notifier() { }
            virtual void operator()(const Transaction* blocker) = 0;
        };

        /**
         * Tracks locking statistics.
         */
        class LockStats {
        public:
        LockStats()
            : _numRequests(0)
            , _numPreexistingRequests(0)
            , _numSameRequests(0)
            , _numBlocks(0)
            , _numDeadlocks(0)
            , _numDowngrades(0)
            , _numUpgrades(0)
            , _numMillisBlocked(0) { }

            void incRequests() { _numRequests++; }
            void incPreexisting() { _numPreexistingRequests++; }
            void incSame() { _numSameRequests++; }
            void incBlocks() { _numBlocks++; }
            void incDeadlocks() { _numDeadlocks++; }
            void incDowngrades() { _numDowngrades++; }
            void incUpgrades() { _numUpgrades++; }
            void incTimeBlocked(size_t numMillis ) { _numMillisBlocked += numMillis; }

            size_t getNumRequests() const { return _numRequests; }
            size_t getNumPreexistingRequests() const { return _numPreexistingRequests; }
            size_t getNumSameRequests() const { return _numSameRequests; }
            size_t getNumBlocks() const { return _numBlocks; }
            size_t getNumDeadlocks() const { return _numDeadlocks; }
            size_t getNumDowngrades() const { return _numDowngrades; }
            size_t getNumUpgrades() const { return _numUpgrades; }
            size_t getNumMillisBlocked() const { return _numMillisBlocked; }

            LockStats& operator+=(const LockStats& other);
            std::string toString() const;

        private:
            // total number of resource requests. >= number or resources requested.
            size_t _numRequests;

            // the number of times a resource was requested when there
            // was a pre-existing request (possibly compatible)
            size_t _numPreexistingRequests;

            // the number of times a transaction requested the same resource in the
            // same mode while already holding a lock on that resource
            size_t _numSameRequests;

            // the number of times a resource request blocked.  This is usually 
            // because of a conflicting pre-existing request, but could be because
            // of a policy like READERS_ONLY
            size_t _numBlocks;

            size_t _numDeadlocks;
            size_t _numDowngrades;
            size_t _numUpgrades;

            // aggregates time requests spent blocked.
            size_t _numMillisBlocked;
        };


    public:

        /**
         * Singleton factory - retrieves a common instance of LockManager
         */
        static LockManager& getSingleton();

        /**
         * It's possibly useful to allow multiple LockManagers for non-overlapping sets
         * of resources, so the constructor is left public.  Eventually we may want
         * to enforce a singleton pattern.
         */
        explicit LockManager(const Policy& policy=kPolicyFirstCome);
        ~LockManager();

        /**
         * Change the current Policy.  For READERS/kPolicyWritersOnly, this
         * call may block until all current writers/readers have released their locks.
         */
        void setPolicy(Transaction* tx, const Policy& policy, Notifier* notifier = NULL);

        /**
         * Get the current policy
         */
        Policy getPolicy() const;

        /**
         * Initiate a shutdown, specifying a period of time to quiesce.
         *
         * During this period, existing transactions can continue to acquire resources,
         * but new transaction requests will throw AbortException.
         *
         * After quiescing, any new requests will throw AbortException
         */
        void shutdown(const unsigned& millisToQuiesce = 1000);


        /**
         * acquire a resource in a mode.
         * can throw AbortException to avoid deadlock.
         * if acquisition would block, the notifier is called before sleeping.
         */
        void acquire(Transaction* requestor,
                     const Locking::LockMode& mode,
                     const ResourceId& childId,
                     Notifier* notifier = NULL);

        /**
         * acquire a designated lock on a childe resource subsumed by a parent resource
         * checks that the parent resource is also locked.  if the locking modes
         * match, the parent's lock obviates the need for locking the child resource.
         * usually called from RAII lock objects.  May throw AbortException to avoid deadlock.
         * if acquisition would block, the notifier is called before sleeping.
         */
        void acquireUnderParent(Transaction* requestor,
                                const Locking::LockMode& mode,
                                const ResourceId& childId,
                                const ResourceId& parentId,
                                Notifier* notifier = NULL);

        /**
         * acquire a designated lock.
         * usually called from RAII lock objects.  May throw AbortException to avoid deadlock.
         * if acquisition would block, the notifier is called before sleeping.
         */
        void acquireLock(LockRequest* request, Notifier* notifier = NULL);

        /**
         * acquire a designated lock on a childe resource subsumed by a parent resource
         * checks that the parent resource is also locked.  if the locking modes
         * match, the parent's lock obviates the need for locking the child resource.
         * usually called from RAII lock objects.  May throw AbortException to avoid deadlock.
         * if acquisition would block, the notifier is called before sleeping.
         */
        void acquireLockUnderParent(LockRequest* childLock,
                                    const ResourceId& parentId,
                                    Notifier* notifier = NULL);

        /**
         * release a ResourceId.
         * The mode here is just the mode that applies to the resId
         */
        LockStatus release(const Transaction* holder,
                           const Locking::LockMode& mode,
                           const ResourceId& resId);

        /**
         * releases the lock returned by acquire.  should perhaps replace above?
         */
        LockStatus releaseLock(LockRequest* request);

        /**
         * relinquish locks acquired by a transactions within a scope
         * whose count is zero.
         */
        void relinquishScopedTxLocks(LockRequest* locks);

        /**
         * returns a copy of the stats that exist at the time of the call
         */
        LockStats getStats() const;

        /**
         * slices the space of ResourceIds and TransactionIds into
         * multiple partitions that can be separately guarded to
         * spread the cost of mutex locking.
         */
        static unsigned partitionResource(const ResourceId& resId);
        static unsigned partitionTransaction(unsigned txId);



        // --- for testing and logging

        std::string toString() const;

        /**
         * test whether a Transaction has locked a ResourceId in a mode.
         * most callers should use acquireOne instead
         */
        bool isLocked(const Transaction* holder,
                      const Locking::LockMode& mode,
                      const ResourceId& resId) const;

    private: // alphabetical

        /**
         * main workhorse for acquiring locks on resources, blocking
         * or aborting on conflict
         *
         * throws AbortException on deadlock
         */
        void _acquireInternal(LockRequest* lr,
                              LockRequest* queue,
                              LockRequest* conflictPosition,
                              ResourceStatus resourceStatus,
                              Notifier* notifier,
                              boost::unique_lock<boost::mutex>& guard);

        /**
         * adds a conflicting lock request to the list of requests for a resource
         * using the Policy.  Called by acquireInternal
         */
        void _addLockToQueueUsingPolicy(LockRequest* lr,
                                        LockRequest* queue,
                                        LockRequest*& position /* in/out */);

        /**
         * when inserting a new lock request into the middle of a queue,
         * add any remaining incompatible requests in the queue to the
         * new lock request's set of waiters... for future deadlock detection
         */
        void _addWaiters(LockRequest* blocker,
                         LockRequest* nextLock,
                         LockRequest* lastLock);

        /**
         * returns true if a newRequest should be honored before an oldRequest according
         * to the lockManager's policy.  Used by acquire to decide whether a new share request
         * conflicts with a previous upgrade-to-exclusive request that is blocked.
         */
        bool _comesBeforeUsingPolicy(const Transaction* newReqTx,
                                     const Locking::LockMode& newReqMode,
                                     const LockRequest* oldReq) const;

        /**
         * determine whether a resource request would conflict with an existing lock
         * set the position to the first possible insertion point, which is usually
         * the position of the first conflict, or the end of the queue, or to an existing lock
         */
        ResourceStatus _conflictExists(Transaction* requestor,
                                       const Locking::LockMode& mode,
                                       const ResourceId& resId,
                                       unsigned slice,
                                       LockRequest*& position /* in/out */);

        /**
         * looks for an existing LockRequest that matches the four input params
         * if not found, sets outLid to zero and returns a reason, otherwise
         * sets outLock to the LockRequest that matches and returns kLockFound
         */
        LockStatus _findLock(const Transaction* requestor,
                             const Locking::LockMode& mode,
                             const ResourceId& resId,
                             unsigned slice,
                             LockRequest*& outLock /* output */) const;

        /**
         * find the first lock on resId or NULL if no locks
         */
        LockRequest* _findQueue(unsigned slice, const ResourceId& resId) const;

	/**
	 * @return status of requested resource id
	 * set conflictPosition on output if conflict
	 * update several status variables
	 */
	ResourceStatus _getConflictInfo(Transaction* requestor,
					const Locking::LockMode& mode,
					const ResourceId& resId,
					unsigned slice,
					LockRequest* queue,
					LockRequest*& conflictPosition /*in/out*/);

        /**
         * maintain the resourceLocks queue
         */
        void _push_back(LockRequest* lr);
        void _removeFromResourceQueue(LockRequest* lr);


        /**
         * called by public ::release and internally _releaseTxLocks
         * assumes caller as acquired a mutex.
         */
        LockStatus _releaseInternal(LockRequest* lr);

        /**
         * called at start of public APIs, throws exception
         * if quiescing period has expired, or if xid is new
         */
        void _throwIfShuttingDown(const Transaction* tx=NULL) const;


    private:
        // support functions for changing policy to/from read/write only

        void _incStatsForMode(const Locking::LockMode& mode) {
            if (Locking::kShared==mode)
                _numCurrentActiveReadRequests.fetchAndAdd(1);
            else if (Locking::kExclusive==mode)
                _numCurrentActiveWriteRequests.fetchAndAdd(1);
        }
        void _decStatsForMode(const Locking::LockMode& mode) {
            if (Locking::kShared==mode)
                _numCurrentActiveReadRequests.fetchAndSubtract(1);
            else if (Locking::kExclusive==mode)
                _numCurrentActiveWriteRequests.fetchAndSubtract(1);
        }

        unsigned _numActiveReads() const { return _numCurrentActiveReadRequests.load(); }
        unsigned _numActiveWrites() const { return _numCurrentActiveWriteRequests.load(); }


    private:

        // The Policy controls which requests should be honored first.  This is
        // used to guide the position of a request in a list of requests waiting for
        // a resource.
        //
        // XXX At some point, we may want this to also guide the decision of which
        // transaction to abort in case of deadlock.  For now, the transaction whose
        // request would lead to a deadlock is aborted.  Since deadlocks are rare,
        // careful choices may not matter much.
        //
        Policy _policy;

        // transaction which last set policy, for reporting cause of conflicts. not owned
        Transaction* _policySetter;

        // synchronizes access to the lock manager, which is shared across threads
        static const unsigned kNumResourcePartitions = 1;

        mutable boost::mutex _resourceMutexes[kNumResourcePartitions];

        // for controlling access to stats & policy
        mutable boost::mutex _mutex;

        // for blocking when setting kPolicyReadersOnly or kPolicyWritersOnly policy
        boost::condition_variable _policyLock;

        // only meaningful when _policy == SHUTDOWN
        bool _shuttingDown;
        int _millisToQuiesce;
        Timer _timer;

        // Lists of lock requests associated with a resource,
        //
        // The lock-request lists have two sections.  Some number (at least one) of requests
        // at the front of a list are "active".  All remaining lock requests are blocked by
        // some earlier (not necessarily active) lock request, and are waiting.  The order
        // of lock request in the waiting section is determined by the LockPolicty.
        // The order of lock request in the active/front portion of the list is irrelevant.
        //
        std::map<ResourceId, LockRequest*> _resourceLocks[kNumResourcePartitions];

#ifdef REGISTER_TRANSACTIONS
        std::set<Transaction*> _activeTransactions[kNumTransactionPartitions];
#endif

        // used to track conflicts due to kPolicyReadersOnly or WritersOnly
        Transaction* _systemTransaction;

        // stats
        LockStats _stats[kNumResourcePartitions];

        // used when changing policy to/from Readers/Writers Only
        AtomicWord<uint32_t> _numCurrentActiveReadRequests;
        AtomicWord<uint32_t> _numCurrentActiveWriteRequests;
    };
} // namespace mongo
