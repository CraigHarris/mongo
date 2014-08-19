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
#include <boost/thread/recursive_mutex.hpp>
#include <iterator>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "mongo/platform/compiler.h"
#include "mongo/platform/cstdint.h"
#include "mongo/platform/atomic_word.h"
#include "mongo/base/string_data.h"
#include "mongo/util/timer.h"

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

    class ResourceId {
    public:
        ResourceId() : _rid(0 /* should be kReservedResourceId */) { }

        /**
         * Construct resource identifiers without regard to their scope.  Used for
         * top-level resources (global locks), or when collisions (same resource id
         * allocated by multiple RecordStores) are unlikely.
         */
        ResourceId(uint64_t rid) : _rid(rid) { }
        explicit ResourceId(const void* loc) : _rid(reinterpret_cast<uint64_t>(loc)) { }

        /**
         * Construct resource identifiers taking into account their scope.
         * The native mongo record store allocates the same recordId for the first document
         * in every collection. These constructors avoid locking skew in such cases.
         */
        ResourceId(uint64_t resourceId, const StringData& scope);
        ResourceId(uint64_t resourceId, const void* resourceIdAllocator);

        bool operator<(const ResourceId& other) const { return _rid < other._rid; }
        bool operator==(const ResourceId& other) const { return _rid == other._rid; }

    private:
        uint64_t _rid;
    };
    static const ResourceId kReservedResourceId;

    /**
     * LockModes are used to control access to resources
     *
     *    kIntentShared: acquired on containers to indicate intention to acquire
     *                   shared lock on contents.  Prevents container from being
     *                   modified while working with contents. should be acquired
     *                   on databases and collections before accessing documents
     *
     *    kIntentExclusive: similar to above, prevents container from being locked
     *                      in a way that would block exclusive locks on contents
     *                      should be acquired on databases and collections before
     *                      modifying their content.
     *
     *    kShared: acquired on either containers or leaf resources before viewing
     *             their state.  prevents concurrent modifications to their state
     *
     *    kSIX: not sure we have a use case for this
     *
     *    kUpdate: an optimization that reduces deadlock frequency compared to
     *             acquiring kShared and then upgrading to kExclusive.  Acts as
     *             a kShared lock, and must be upgraded to kExclusive before
     *             modifying the locked resource.
     *
     *    kBlockExclusive: acquired on a container to block exclusive access to
     *                     its contents.  If multiple kBlockExclusive locks are
     *                     acquired, exclusive access is blocked from the time
     *                     the first lock is acquired until the last lock is
     *                     released.  Also blocks kUpdate locks
     *
     *    kExclusive: acquired before modifying a resource, blocks all other
     *                concurrent access to the resource.
     *                   
     */

    /**
     * LockMode Compatibility Matrix:
     *                                           Granted Mode
     *                               +-----+-----+-----+-----+-----+-----+-----+
     * Requested Mode                | IS  | IX  |  S  | SIX |  U  | BX  |  X  |
     * --------------                +-----+-----+-----+-----+-----+-----+-----+
     * kIntentShared(IS)             | ok  | ok  | ok  | ok  | ok  | ok  |  -  |
     *                               +-----+-----+-----+-----+-----+-----+-----+
     * kIntentExclusive(IX)          | ok  | ok  |  -  |  -  |  -  |  -  |  -  |
     *                               +-----+-----+-----+-----+-----+-----+-----+
     * kShared(S)                    | ok  |  -  | ok  |  -  | ok  | ok  |  -  |
     *                               +-----+-----+-----+-----+-----+-----+-----+
     * kSharedOrIntentExclusive(SIX) | ok  |  -  |  -  |  -  |  -  |  -  |  -  |
     *                               +-----+-----+-----+-----+-----+-----+-----+
     * kUpdate(U)                    | ok  |  -  | ok  |  -  |  -  |  -  |  -  |
     *                               +-----+-----+-----+-----+-----+-----+-----+
     * kBlockExclusive(BX)           | ok  |  -  | ok  |  -  |  -  | ok  |  -  |
     *                               +-----+-----+-----+-----+-----+-----+-----+
     * kExclusive(X)                 |  -  |  -  |  -  |  -  |  -  |  -  |  -  |
     *                               +-----+-----+-----+-----+-----+-----+-----+
     */

    /**
     * Upgrades:
     *     IS  -> {S, IX}
     *     S   -> {SIX, U}
     *     IX  -> {SIX}
     *     SIX -> X
     *     U   -> X
     *     BX  -> X
     */

    enum LockMode {
        kIntentShared,
        kIS = kIntentShared,

        kIntentExclusive,
        kIX = kIntentExclusive,

        kShared,
        kS = kShared,

        kSharedIntentExclusive,
        kSIX = kSharedIntentExclusive,

        kUpdate,
        kU = kUpdate,

        kBlockExclusive,
        kBX = kBlockExclusive,

        kExclusive,
        kX = kExclusive
    };


    class Transaction;
    class LockManager;

    /**
     * Data structure used to record a resource acquisition request
     */
    class LockRequest {
    public:
        LockRequest(const ResourceId& resId,
                    const LockMode& mode,
                    Transaction* requestor,
                    bool heapAllocated = false);


        ~LockRequest();

        bool matches(const Transaction* tx,
                     const LockMode& mode,
                     const ResourceId& resId) const;

        bool isBlocked() const;
        bool isActive() const { return !isBlocked(); }
        bool shouldAwake();

        std::string toString() const;

        // insert/append in resource chain
        void insert(LockRequest* lr);
        void append(LockRequest* lr);

        // transaction that made this request (not owned)
        Transaction* requestor;

        // shared or exclusive use
        const LockMode mode;

        // resource requested
        const ResourceId resId;

        // a hash of resId modulo kNumResourcePartitions
        // used to mitigate cost of mutex locking
        unsigned slice;

        // it's an error to exit top-level scope with locks
        // remaining that were acquired in-scope. it's also
        // an error to release a lock acquired out-of-scope
        // inside a scope
        bool acquiredInScope;

        // number of times a tx requested resource in this mode
        // lock request will be deleted when count goes to 0
        size_t count;

        // number of existing things blocking this request
        // usually preceding requests on the queue, but also policy
        size_t sleepCount;

        // ResourceLock classes (see below) using the RAII pattern
        // allocate LockRequests on the stack.
        bool heapAllocated;

        // lock requests are chained by their resource
        LockRequest* nextOnResource;
        LockRequest* prevOnResource;

        // lock requests are also chained by their requesting transaction
        LockRequest* nextOfTransaction;
        LockRequest* prevOfTransaction;
    };

    /**
     * Data structure used to describe resource requestors,
     * used for conflict resolution, deadlock detection, and abort
     */
    class Transaction {
    public:

        /**
         * thrown when Transaction::abort is called, often to avoid deadlock.
         */
        class AbortException : public std::exception {
        public:
            const char* what() const throw ();
        };

    public:

        Transaction(unsigned txId=0, int priority=0);
        ~Transaction();

        /**
         * transactions are identified by an id
         */
        Transaction* setTxIdOnce(unsigned txId);
        unsigned getTxId() const { return _txId; }

        /**
         * scope corresponds to UnitOfWork nesting level, but may apply to
         * read transactions of 3rd-party storage engines.
         */
        bool inScope() const { return _scopeLevel != 0; }
        void enterScope();
        void exitScope(LockManager* lm=NULL);

        void releaseLocks(LockManager* lm);
        MONGO_COMPILER_NORETURN void abort();
        

        /**
         * override default LockManager's default Policy for a transaction.
         *
         * positive priority moves transaction's resource requests toward the front
         * of the queue, behind only those requests with higher priority.
         *
         * negative priority moves transaction's resource requests toward the back
         * of the queue, ahead of only those requests with lower priority.
         *
         * zero priority uses the LockManager's default Policy
         */
        void setPriority(int newPriority);
        int getPriority() const;

        /**
         * maintain the queue of lock requests made by the transaction
         */
        void removeLock(LockRequest* lr);
	void addLock(LockRequest* lr);

        /**
         * waiter functions
         */
        void addWaiter(Transaction* waiter);
        size_t numWaiters() const;
        bool hasWaiter(const Transaction* other) const;
        void removeWaiterAndItsWaiters(const Transaction* other);
        void removeAllWaiters();

        /**
         * for sleeping and waking
         */
        void wait(boost::unique_lock<boost::mutex>& guard);
        void wake();

        /**
         * should be age of the transaction.  currently using txId as a proxy.
         */
        bool operator<(const Transaction& other) const;

        /**
         * for debug
         */
        std::string toString() const;

    private:
        friend class LockManager;

        // identify the transaction
        unsigned _txId;

        // 0 ==> outside unit of work.  Used to control relinquishing locks
        size_t _scopeLevel;

        // transaction priorities:
        //     0 => neutral, use LockManager's default _policy
        //     + => high, queue forward
        //     - => low, queue back
        //
        int _priority;

        // synchronize access to transaction's _locks and _waiters
        // which are modified by the lock manager
        mutable boost::recursive_mutex _txMutex;

        // For cleanup and abort processing, references all LockRequests made by a transaction
        LockRequest* _locks;

        // used for waiting and waking
        boost::condition_variable _condvar;

        // For deadlock detection: the set of transactions blocked by another transaction
        // NB: a transaction can only be directly waiting for a single resource/transaction
        // but to facilitate deadlock detection, if T1 is waiting for T2 and T2 is waiting
        // for T3, then both T1 and T2 are listed as T3's waiters.
        //
        // This is a multiset to handle some obscure situations.  If T1 has upgraded or downgraded
        // its lock on a resource, it has two lock requests.  If T2 then requests exclusive
        // access to the same resource, it must wait for BOTH T1's locks to be relased.
        //
        // the max size of the set is ~2*number-concurrent-transactions.  the set is only
        // consulted/updated when there's a lock conflict.  When there are many more documents
        // than transactions, the set will usually be empty.
        //
        std::multiset<const Transaction*> _waiters;
    };

    /**
     *  LockManager is used to control access to resources. Usually a singleton. For deadlock detection
     *  all resources used by a set of transactions that could deadlock, should use one LockManager.
     *
     *  Primary functions are:
     *     acquire    - acquire a resource for shared or exclusive use; may throw Abort.
     *     release    - release a resource previously acquired for shared/exclusive use
     */
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
                     const LockMode& mode,
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
                                const LockMode& mode,
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
                           const LockMode& mode,
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
                      const LockMode& mode,
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
                                        LockRequest*& position);

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
                                     const LockMode& newReqMode,
                                     const LockRequest* oldReq) const;

        /**
         * determine whether a resource request would conflict with an existing lock
         * set the position to the first possible insertion point, which is usually
         * the position of the first conflict, or the end of the queue, or to an existing lock
         */
        ResourceStatus _conflictExists(Transaction* requestor,
                                       const LockMode& mode,
                                       const ResourceId& resId,
                                       unsigned slice,
                                       LockRequest*& position /* in/out */);

        /**
         * looks for an existing LockRequest that matches the four input params
         * if not found, sets outLid to zero and returns a reason, otherwise
         * sets outLock to the LockRequest that matches and returns kLockFound
         */
        LockStatus _findLock(const Transaction* requestor,
                             const LockMode& mode,
                             const ResourceId& resId,
                             unsigned slice,
                             LockRequest*& outLock) const;

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
					const LockMode& mode,
					const ResourceId& resId,
					unsigned slice,
					LockRequest* queue,
					LockRequest*& conflictPosition);

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

        void _incStatsForMode(const LockMode& mode) {
            if (kShared==mode)
                _numCurrentActiveReadRequests.fetchAndAdd(1);
            else if (kExclusive==mode)
                _numCurrentActiveWriteRequests.fetchAndAdd(1);
        }
        void _decStatsForMode(const LockMode& mode) {
            if (kShared==mode)
                _numCurrentActiveReadRequests.fetchAndSubtract(1);
            else if (kExclusive==mode)
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
        static const unsigned kNumResourcePartitions = 16;
        mutable boost::mutex _resourceMutexes[kNumResourcePartitions];

#ifdef REGISTER_TRANSACTIONS
        static const unsigned kNumTransactionPartitions = 16;
        mutable boost::mutex _transactionMutexes[kNumTransactionPartitions];
#endif
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

    /**
     * RAII wrapper around LockManager, for scoped locking
     */
    class ResourceLock {
    public:
        /**
         * acquire & release lock in given mode on requested resource
         */
        ResourceLock(LockManager& lm,
                     Transaction* requestor,
                     const LockMode& mode,
                     const ResourceId& resId,
                     LockManager::Notifier* notifier = NULL);

        /**
         * acquire & release lock in given mode on child resource
         * check that parent resource is locked, and if in matching mode,
         * skip the lock acquisition on the child
         */
        ResourceLock(LockManager& lm,
                     Transaction* requestor,
                     const LockMode& mode,
                     const ResourceId& childResId,
                     const ResourceId& parentResId,
                     LockManager::Notifier* notifier = NULL);

        ~ResourceLock();
    private:
        LockManager& _lm;
        LockRequest _lr;
    };

    class IntentSharedResourceLock : public ResourceLock {
    public:
        /**
         * acquire & release lock in given mode on requested resource
         */
        IntentSharedResourceLock(Transaction* requestor,
                                 const ResourceId& resId,
                                 LockManager::Notifier* notifier = NULL)
            : ResourceLock(LockManager::getSingleton(),
                           requestor,
                           kIntentShared,
                           resId,
                           notifier) { }

        /**
         * acquire & release lock in given mode on child resource
         * check that parent resource is locked, and if in matching mode,
         * skip the lock acquisition on the child
         */
        IntentSharedResourceLock(Transaction* requestor,
                                 const ResourceId& childResId,
                                 const ResourceId& parentResId,
            LockManager::Notifier* notifier = NULL)
            : ResourceLock(LockManager::getSingleton(),
                           requestor,
                           kIntentShared,
                           childResId,
                           parentResId,
                           notifier) { }

        ~IntentSharedResourceLock();
    };

    class IntentExclusiveResourceLock : public ResourceLock {
    public:
        /**
         * acquire & release lock in given mode on requested resource
         */
        IntentExclusiveResourceLock(Transaction* requestor,
                                    const ResourceId& resId,
                                    LockManager::Notifier* notifier = NULL)
            : ResourceLock(LockManager::getSingleton(),
                           requestor,
                           kIntentExclusive,
                           resId,
                           notifier) { }

        /**
         * acquire & release lock in given mode on child resource
         * check that parent resource is locked, and if in matching mode,
         * skip the lock acquisition on the child
         */
        IntentExclusiveResourceLock(Transaction* requestor,
                                    const ResourceId& childResId,
                                    const ResourceId& parentResId,
                                    LockManager::Notifier* notifier = NULL)
            : ResourceLock(LockManager::getSingleton(),
                           requestor,
                           kIntentExclusive,
                           childResId,
                           parentResId,
                           notifier) { }

        ~IntentExclusiveResourceLock() { }
    };

    class SharedResourceLock : public ResourceLock {
    public:
        /**
         * acquire & release shared lock on requested resource
         */
        SharedResourceLock(Transaction* requestor,
                           const ResourceId& resId,
                           LockManager::Notifier* notifier = NULL)
            : ResourceLock(LockManager::getSingleton(),
                           requestor,
                           kShared,
                           resId,
                           notifier) { }

        /**
         * acquire & release lock shared on child resource
         * check that parent resource is locked, and if also shared
         * skip the lock acquisition on the child
         */
        SharedResourceLock(Transaction* requestor,
                           const ResourceId& childResId,
                           const ResourceId& parentResId,
                           LockManager::Notifier* notifier = NULL)
            : ResourceLock(LockManager::getSingleton(),
                           requestor,
                           kShared,
                           childResId,
                           parentResId,
                           notifier) { }

        ~SharedResourceLock() { }
    };

    class UpdateResourceLock : public ResourceLock {
    public:
        /**
         * acquire & release lock update on requested resource
         */
        UpdateResourceLock(Transaction* requestor,
                           const ResourceId& resId,
                           LockManager::Notifier* notifier = NULL)
            : ResourceLock(LockManager::getSingleton(),
                           requestor,
                           kUpdate,
                           resId,
                           notifier) { }

        /**
         * acquire & release lock update on child resource
         * check that parent resource is locked, and if also update
         * skip the lock acquisition on the child
         */
        UpdateResourceLock(Transaction* requestor,
                           const ResourceId& childResId,
                           const ResourceId& parentResId,
                           LockManager::Notifier* notifier = NULL)
            : ResourceLock(LockManager::getSingleton(),
                           requestor,
                           kUpdate,
                           childResId,
                           parentResId,
                           notifier) { }

        ~UpdateResourceLock() { }
    };

    class SharedIntentExclusiveResourceLock : public ResourceLock {
    public:
        /**
         * acquire & release exlusive lock on requested resource
         */
        SharedIntentExclusiveResourceLock(Transaction* requestor,
                              const ResourceId& resId,
                              LockManager::Notifier* notifier = NULL)
            : ResourceLock(LockManager::getSingleton(),
                           requestor,
                           kSharedIntentExclusive,
                           resId,
                           notifier) { }
                

        /**
         * acquire & release exclusive lock on child resource
         * check that parent resource is locked, and if also exclusive
         * skip the lock acquisition on the child
         */
        SharedIntentExclusiveResourceLock(Transaction* requestor,
                              const ResourceId& childResId,
                              const ResourceId& parentResId,
                              LockManager::Notifier* notifier = NULL)
            : ResourceLock(LockManager::getSingleton(),
                           requestor,
                           kSharedIntentExclusive,
                           childResId,
                           parentResId,
                           notifier) { }

        ~SharedIntentExclusiveResourceLock() { }
    };

    class BlockExclusiveResourceLock : public ResourceLock {
    public:
        /**
         * acquire & release exlusive lock on requested resource
         */
        BlockExclusiveResourceLock(Transaction* requestor,
                              const ResourceId& resId,
                              LockManager::Notifier* notifier = NULL)
            : ResourceLock(LockManager::getSingleton(),
                           requestor,
                           kBlockExclusive,
                           resId,
                           notifier) { }
                

        /**
         * acquire & release exclusive lock on child resource
         * check that parent resource is locked, and if also exclusive
         * skip the lock acquisition on the child
         */
        BlockExclusiveResourceLock(Transaction* requestor,
                              const ResourceId& childResId,
                              const ResourceId& parentResId,
                              LockManager::Notifier* notifier = NULL)
            : ResourceLock(LockManager::getSingleton(),
                           requestor,
                           kBlockExclusive,
                           childResId,
                           parentResId,
                           notifier) { }

        ~BlockExclusiveResourceLock() { }
    };

    class ExclusiveResourceLock : public ResourceLock {
    public:
        /**
         * acquire & release exlusive lock on requested resource
         */
        ExclusiveResourceLock(Transaction* requestor,
                              const ResourceId& resId,
                              LockManager::Notifier* notifier = NULL)
            : ResourceLock(LockManager::getSingleton(),
                           requestor,
                           kExclusive,
                           resId,
                           notifier) { }
                

        /**
         * acquire & release exclusive lock on child resource
         * check that parent resource is locked, and if also exclusive
         * skip the lock acquisition on the child
         */
        ExclusiveResourceLock(Transaction* requestor,
                              const ResourceId& childResId,
                              const ResourceId& parentResId,
                              LockManager::Notifier* notifier = NULL)
            : ResourceLock(LockManager::getSingleton(),
                           requestor,
                           kExclusive,
                           childResId,
                           parentResId,
                           notifier) { }

        ~ExclusiveResourceLock() { }
    };
} // namespace mongo
