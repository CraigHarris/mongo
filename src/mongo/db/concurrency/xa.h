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

#include <iterator>
#include <list>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "mongo/platform/compiler.h"
#include "mongo/platform/cstdint.h"
#include "mongo/util/timer.h"

/*
 * LockManager controls access to resources through two functions: acquire and release
 *
 * Resources are either RecordStores, or Records within an RecordStore, identified by a ResourceId.
 * Resources are acquired for either shared or exclusive use, by transactions identified by a TxId.
 * Acquiring Records in any mode implies acquisition of the Record's RecordStore
 *
 * Contention for a resource is resolved by a LockingPolicy, which determines which blocked
 * resource requests to awaken when the blocker releases the resource.
 *
 */

namespace mongo {

    class TxId {
    public:
        TxId() : _xid(0) { }
        TxId(size_t xid) : _xid(xid) { }
        bool operator<(const TxId& other) const { return _xid < other._xid; }
        bool operator==(const TxId& other) const { return _xid == other._xid; }
        operator size_t() const { return _xid; }
        
    private:
        size_t _xid;
    };

    class Transaction {
        
    };


    /**
     *  LockManager is used to control access to resources. Usually a singleton. For deadlock detection
     *  all resources used by a set of transactions that could deadlock, should use one LockManager.
     *
     *  Primary functions are:
     *     acquire    - acquire a resource for shared or exclusive use; may throw Abort.
     *     acquireOne - acquire one of a vector of resources, hopefully without blocking
     *     release    - release a resource previously acquired for shared/exclusive use
     */
    class LockManager {
    public:

        /**
         * thrown primarily when deadlocks are detected, or when LockManager::abort is called.
         * also thrown when LockManager requests are made during shutdown.
         */
        class AbortException : public std::exception {
        public:
            const char* what() const throw ();
        };

        /**
         * LockModes: shared and exclusive
         */
        static const unsigned kShared = 0x0;
        static const unsigned kExclusive = 0x1;

        /**
         * Used to decide which blocked requests to honor first when resource becomes available
         */
        enum Policy {
            kPolicyFirstCome,     // wake the first blocked request in arrival order
            kPolicyReadersFirst,  // wake the first blocked read request(s)
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
         * returned by ::find_lock, and ::release and, mostly for testing
         * explains why a lock wasn't found or released
         */
        enum LockStatus {
            kLockFound,             // found a matching lock request
            kLockReleased,          // released requested lock
            kLockCountDecremented,  // decremented lock count, but didn't release
	    kLockIdNotFound,        // no locks with this id
            kLockContainerNotFound, // no locks on the container for the resource
            kLockResourceNotFound,  // no locks on the resource
            kLockModeNotFound       // locks on the resource, but not of the specified mode
        };

        typedef size_t LockId; // valid LockIds are > 0
        static const LockId kReservedLockId = 0;

        /**
         * Used to do something just before an acquisition request blocks.
         *
         * XXX: should perhaps define Notifier as a functor so C++11 lambda's match
         * for the test.cpp, it was convenient for the call operator to access private
         * state, which is why we're using the class formulation for now
         */
        class Notifier {
        public:
             virtual ~Notifier() { }
             virtual void operator()(const TxId& blocker) = 0;
        };

        /**
         * Tracks locking statistics.  For now, just aggregated across all resources/TxIds
         * Eventually might keep per TxId and/or per Resource, to facilitate identifying
         * hotspots and problem transactions.
         */
        class LockStats {
        public:
            LockStats()
                : _numRequests(0)
                , _numPreexistingRequests(0)
                , _numBlocks(0)
                , _numDeadlocks(0)
                , _numDowngrades(0)
                , _numUpgrades(0)
                , _numMillisBlocked(0)
                , _numCurrentActiveReadRequests(0)
                , _numCurrentActiveWriteRequests(0) { }

            void incRequests() { _numRequests++; }
            void incPreexisting() { _numPreexistingRequests++; }
            void incBlocks() { _numBlocks++; }
            void incDeadlocks() { _numDeadlocks++; }
            void incDowngrades() { _numDowngrades++; }
            void incUpgrades() { _numUpgrades++; }
            void incTimeBlocked(size_t numMillis ) { _numMillisBlocked += numMillis; }

	    void incStatsForMode(const unsigned mode) {
		0==mode ? incActiveReads() : incActiveWrites();
	    }
	    void decStatsForMode(const unsigned mode) {
		0==mode ? decActiveReads() : decActiveWrites();
	    }

            void incActiveReads() { _numCurrentActiveReadRequests++; }
            void decActiveReads() { _numCurrentActiveReadRequests--; }
            void incActiveWrites() { _numCurrentActiveWriteRequests++; }
            void decActiveWrites() { _numCurrentActiveWriteRequests--; }

            unsigned numActiveReads() const { return _numCurrentActiveReadRequests; }
            unsigned numActiveWrites() const { return _numCurrentActiveWriteRequests; }

            size_t getNumRequests() const { return _numRequests; }
            size_t getNumPreexistingRequests() const { return _numPreexistingRequests; }
            size_t getNumBlocks() const { return _numBlocks; }
            size_t getNumDeadlocks() const { return _numDeadlocks; }
            size_t getNumDowngrades() const { return _numDowngrades; }
            size_t getNumUpgrades() const { return _numUpgrades; }
            size_t getNumMillisBlocked() const { return _numMillisBlocked; }

        private:
            size_t _numRequests;
            size_t _numPreexistingRequests;
            size_t _numBlocks;
            size_t _numDeadlocks;
            size_t _numDowngrades;
            size_t _numUpgrades;
            size_t _numMillisBlocked;
            unsigned long _numCurrentActiveReadRequests;
            unsigned long _numCurrentActiveWriteRequests;
        };


    public:

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
         void setPolicy(const TxId& xid, const Policy& policy, Notifier* notifier = NULL);

         /**
          * Get the current policy
          */
         Policy getPolicy() const;

         /**
          * Who set the current policy.  Of use when the Policy is ReadersOnly
          * and we want to find out who is blocking a writer.
          */
         TxId getPolicySetter() const;

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
         * For multi-level resource container hierarchies, the caller can optionally
         * identify the ancestry relationships.  For example, if database resources
         * contained collection resources, which contained document resources, this
         * API could be used to relate a specific collection to its parent database.
         *
         * Once this is done, a call to acquire a document in a particular collection
         * could also be used to lock the containing database, without supplying the
         * resourceId for the database.
         */
         void setParent(const ResourceId& container, const ResourceId& parent);


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
         void setTransactionPriority(const TxId& xid, int priority);
         int  getTransactionPriority(const TxId& xid) const;


        /**
         * acquire a resource nested in a container, in a mode.
         *
         * If setParent was previously called to relate container to its parent,
         * then both the container and its identified ancestors will be locked.
         * 
         * the locking mode parameter is a bit vector.  The least significant bit
         * describes whether the resource is locked shared (0) or exclusive (1).
         * the next least significant bit describes the lock mode of the container,
         * and so on for the container's ancestors.  
         *
         * can throw AbortException
         *
         */
         LockId acquire(const TxId& requestor,
                        const uint32_t& mode,
                        const ResourceId& container,
                        const ResourceId& resId,
                        Notifier* notifier = NULL);

        /**
         * acquire a hierarchical resource, locking it and its ancestors in
         * specified modes.
         *
         * functionally equivalent to the previous call, without needing calls
         * to setParent of ancestral containers.
         *
         * the first ResourceId in the resIdPath is the resource requested,
         * the next ResourceId in the resIdPath is the resource's container,
         * followed by the container's parent, etc.
         *
         * the first mode specifies whether to acquire the resource shared or
         * exclusive, the next mode in modes is for the resource's container,
         * and so on.  If modes is shorter thant resIdPath, remaining ancestors
         * are acquired for shared use.
         */
         LockId acquire(const TxId& requestor,
                        const std::vector<unsigned>& modes,
                        const std::vector<ResourceId>& resIdPath,
                        Notifier* notifier = NULL);

        /**
         * for bulk operations:
         * acquire one of a vector of ResourceIds in a mode,
         * hopefully without blocking, return index of 
         * acquired ResourceId, or -1 if vector was empty
         */
         int acquireOne(const TxId& requestor,
                        const uint32_t& mode,
                        const ResourceId& container,
                        const std::vector<ResourceId>& records,
                        Notifier* notifier = NULL);

        /**
         * release a ResourceId in a container.
         * The mode here is just the mode that applies to the resId
         * The modes that applied to the ancestor containers are
         * already known
         */
         LockStatus release(const TxId& holder,
                            const uint32_t& mode,
                            const ResourceId& container,
                            const ResourceId& resId);

        /**
         * releases the lock returned by acquire.  should perhaps replace above?
         */
         LockStatus releaseLock(const LockId& lid);

        /**
         * release all resources acquired by a transaction
         * returns number of locks released
         */
         size_t release(const TxId& holder);

        /**
         * called internally for deadlock
         * possibly called publicly to stop a long transaction
         * also used for testing
         */
        MONGO_COMPILER_NORETURN void abort(const TxId& goner);

        /**
         * returns a copy of the stats that exist at the time of the call
         */
        LockStats getStats() const;



        // --- for testing and logging

        std::string toString() const;

        /**
         * test whether a TxId has locked a nested ResourceId in a mode
         */
         bool isLocked(const TxId& holder,
                       const unsigned& mode,
                       const ResourceId& parentId,
                       const ResourceId& resId) const;

    protected:

        /**
         * Data structure used to record a resource acquisition request
         */
        class LockRequest {
        public:
            LockRequest(const TxId& xid,
                        const unsigned& mode,
                        const ResourceId& container,
                        const ResourceId& resId);

            ~LockRequest();

	    bool matchesResourceAndContainer(const TxId& xid,
					     const unsigned& mode,
					     const ResourceId& parentId,
					     const ResourceId& resId) const;

            bool matchesResourceInQueue(const TxId& xid,
					const unsigned& mode,
					const ResourceId& resId) const;

            bool isBlocked() const;
            bool shouldAwake();

            std::string toString() const;

            // uniquely identifies a LockRequest
            const LockId lid;

            // identifies the parent of this resource, or zero
            LockId parentLid;

            // transaction that made this request
            const TxId xid;

            // shared or exclusive use
            const unsigned mode;

            // container of the resource, or 0 if top-level
            const ResourceId container;

            // resource requested
            const ResourceId resId;

            // number of times xid requested this resource in this mode
            // request will be deleted when count goes to 0
            size_t count;

            // number of existing things blocking this request
            // usually preceding requests on the queue, but also policy
            size_t sleepCount;
                                       
            // used for waiting and waking
            boost::condition_variable lock;
        };
        
        typedef std::multiset<TxId> Waiters;
        typedef std::map<TxId, Waiters*> WaitersMap;
        typedef std::map<TxId, std::set<LockId>*> TxLocks;
        typedef std::map<ResourceId, std::list<LockId>*> ResourceLocks;
        typedef std::map<ResourceId, ResourceLocks > ContainerLocks;
        typedef std::map<LockId, LockRequest*> LockMap;
        typedef std::map<TxId, std::set<LockId>*> TxLockMap;

    private: // alphabetical

        /**
         * called by public ::abort and internally upon deadlock
         * releases all locks acquired by goner, notify's any
         * transactions that were waiting, then throws AbortException
         */
        MONGO_COMPILER_NORETURN void _abortInternal(const TxId& goner);

        /**
         * main workhorse for acquiring locks on resources, blocking
         * or aborting on conflict
         *
         * returns a non-zero LockId, or throws AbortException on deadlock
         *
         */
        LockId _acquireInternal(const TxId& requestor,
                                const unsigned& mode,
                                const ResourceId& containerLid,
                                const ResourceId& resId,
                                Notifier* notifier,
                                boost::unique_lock<boost::mutex>& guard);

        /**
         * adds a conflicting lock request to the list of requests for a resource
         * using the Policy.  Called by acquireInternal
         */
        void _addLockToQueueUsingPolicy(LockRequest* lr,
                                        std::list<LockId>* queue,
                                        std::list<LockId>::iterator& position);

        /**
         * set up for future deadlock detection, called from acquire
         */
        void _addWaiter(const TxId& blocker, const TxId& waiter);

        /**
         * when inserting a new lock request into the middle of a queue,
         * add any remaining incompatible requests in the queue to the
         * new lock request's set of waiters... for future deadlock detection
         */
        void _addWaiters(LockRequest* blocker,
                         std::list<LockId>::iterator nextLockId,
                         std::list<LockId>::iterator lastLockId);

        /**
         * returns true if a newRequest should be honored before an oldRequest according
         * to the lockManager's policy.  Used by acquire to decide whether a new share request
         * conflicts with a previous upgrade-to-exclusive request that is blocked.
         */
        bool _comesBeforeUsingPolicy(const TxId& newReqXid,
                                     const unsigned& newReqMode,
                                     const LockRequest* oldReq) const;

        /**
         * determine whether a resource request would conflict with an existing lock
         * set the position to the first possible insertion point, which is usually
         * the position of the first conflict, or the end of the queue, or to an existing lock
         */
        ResourceStatus _conflictExists(const TxId& requestor,
                                       const unsigned& mode,
                                       const ResourceId& resId,
                                       std::list<LockId>* queue,
                                       std::list<LockId>::iterator& position /* in/out */);

        /**
         * looks for an existing LockRequest that matches the four input params
         * if not found, sets outLid to zero and returns a reason, otherwise
         * sets outLid to the LockId that matches and returns kLockFound
         */
        LockStatus _findLock(const TxId& requestor,
                             const unsigned& mode,
                             const ResourceId& parentId,
                             const ResourceId& resId,
                             LockId* outLid) const;

        /**
         * called externally by getTransactionPriority
         * and internally by addLockToQueueUsingPolicy
         */
        int _getTransactionPriorityInternal(const TxId& xid) const;

        /**
         * returns true if acquire would return without waiting
         * used by acquireOne
         */
        bool _isAvailable(const TxId& requestor,
                          const unsigned& mode,
                          const ResourceId& parentId,
                          const ResourceId& resId) const;

        /**
         * called by public ::release and internally by abort.
         * assumes caller as acquired a mutex.
         */
        LockStatus _releaseInternal(const LockId& lid);

        /**
         * called at start of public APIs, throws exception
         * if quiescing period has expired, or if xid is new
         */
        void _throwIfShuttingDown(const TxId& xid = 0 ) const;


    private:

        // The Policy controls which requests should be honored first.  This is
        // used to guide the position of a request in a list of requests waiting for
        // a resource or resource container.
        //
        // XXX At some point, we may want this to also guide the decision of which
        // transaction to abort in case of deadlock.  For now, the transaction whose
        // request would lead to a deadlock is aborted.  Since deadlocks are rare,
        // careful choices may not matter much.
        //
        Policy _policy;
        TxId _policySetter;

        // synchronizes access to the lock manager, which is shared across threads
        mutable boost::mutex _mutex;

        // for blocking when setting kPolicyReadersOnly or kPolicyWritersOnly policy
        boost::condition_variable _policyLock;

        // only meaningful when _policy == SHUTDOWN
        bool _shuttingDown;
        int _millisToQuiesce;
        Timer _timer;

        // owns the LockRequest*
        std::map<LockId, LockRequest*> _locks;

        // maps containerId -> parentId
        std::map<ResourceId, ResourceId> _containerAncestry;

        // Lists of lock requests associated with a resource,
        // map lowest-level-container to (map of resourceId to list of lock requests)
        //
        // The lock-request lists have two sections.  Some number (at least one) of requests 
        // at the front of a list are "active".  All remaining lock requests are blocked by
        // some earlier (not necessarily active) lock request, and are waiting.  The order
        // of lock request in the waiting section is determined by the LockPolicty.
        // The order of lock request in the active/front portion of the list is irrelevant.
        //
        std::map<ResourceId, std::map<ResourceId, std::list<LockId>*> > _resourceLocks;

        // For cleanup and abort processing, references all LockRequests made by a transaction
        std::map<TxId, std::set<LockId>*> _xaLocks;

        // For deadlock detection: the set of transactions blocked by another transaction
        // NB: a transaction can only be directly waiting for a single resource/transaction
        // but to facilitate deadlock detection, if T1 is waiting for T2 and T2 is waiting
        // for T3, then both T1 and T2 are listed as T3's waiters.  
        std::map<TxId, std::multiset<TxId>*> _waiters;

        // track transactions that have aborted, and don't accept further 
        // lock requests from them (which shouldn't happen anyway).
        //
        // XXX: this set can grow without bound in the pathological case.  One way to deal
        // with it is to track the oldest active transaction (which may or may not be the
        // smallest TxId value), and flush older values from this set and ignore older values
        // in new requests without consulting this set.
        std::set<TxId> _abortedTxIds;

        // transaction priorities:
        //     0 => neutral, use LockManager's default _policy
        //     + => high, queue forward
        //     - => low, queue back
        //
        std::map<TxId, int> _txPriorities;

        // stats, but also used internally
        LockStats _stats;
    };

    /**
     * RAII wrapper around LockManager, for scoped locking
     */
    class ResourceLock {
    public:
        ResourceLock(LockManager& lm,
                     const TxId& requestor,
                     const uint32_t& mode,
                     const ResourceId& parentId,
                     const ResourceId& resId,
                     LockManager::Notifier* notifier = NULL);

        ~ResourceLock();
    private:
        LockManager& _lm;
        LockManager::LockId _lid;
    };
} // namespace mongo
