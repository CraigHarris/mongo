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
#include <list>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "mongo/db/structure/record_store.h"
#include "mongo/util/log.h"

/*
 * LockMgr controls access to resources through two functions: acquire and release
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
    typedef size_t TxId;        // identifies requesting transaction
    typedef size_t ResourceId;  // identifies requested resource, 

    class AbortException : public std::exception {
    public:
        AbortException() {}
        const char* what() const throw () { return "AbortException"; }
    };

    /**
     *  LockMgr is used to control access to resources. Usually a singleton. For deadlock detection
     *  all resources used by a set of transactions that could deadlock, should use one LockMgr.
     *
     *  Primary functions are:
     *     acquire    - acquire a resource for shared or exclusive use; may throw Abort.
     *     acquireOne - acquire one of a vector of resources, hopefully without blocking
     *     release    - release a resource previously acquired for shared/exclusive use
     */

    class LockMgr {
    public:

        /**
         * LockModes: shared and exclusive
         */
        static const unsigned kShared = 0x0;
        static const unsigned kExclusive = 0x1;

        /**
         * Used to decide which blocked requests to honor first when resource becomes available
         */
        enum LockingPolicy {
            FIRST_COME,
            READERS_FIRST,
            OLDEST_TX_FIRST,
            BIGGEST_BLOCKER_FIRST,
            READERS_ONLY,
            WRITERS_ONLY,
            SHUTDOWN_QUIESCE,
            SHUTDOWN
        };

        /** 
         * returned by ::release, mostly for testing
         */
        enum LockStatus {
            FOUND,
            RELEASED,
            COUNT_DECREMENTED,
            CONTAINER_NOT_FOUND,
            RESOURCE_NOT_FOUND,
            RESOURCE_NOT_FOUND_IN_MODE
        };

        typedef size_t LockId;

        /**
         * Data structure used to record a resource acquisition request
         */
        class LockRequest {
        public:
            LockRequest( const TxId& xid,
                         const unsigned& mode,
                         const ResourceId& container,
                         const ResourceId& resId);

            virtual ~LockRequest();

            bool matches( const TxId& xid,
                          const unsigned& mode,
                          const ResourceId& parentId,
                          const ResourceId& resId );

            std::string toString( ) const;

            static LockId nextLid;     // unique id for each request. zero not used

            bool sleep;                // true if waiting, false if resource acquired
            LockId parentLid;          // the LockId of the parent of this resource, or zero
            LockId lid;                // 
            TxId xid;                  // transaction that made this request
            unsigned mode;             // shared or exclusive use
            ResourceId container;      // container of the resource, or 0 if top-level
            ResourceId resId;          // resource requested
            size_t count;              // # times xid requested this resource in this mode
                                       // request will be deleted when count goes to 0

            boost::condition_variable lock; // used for waiting and waking
        };

        /**
         * Used to do something just before an acquisition request blocks.
         */
        class Notifier {
        public:
            virtual void operator()(const TxId& blocker) = 0;
            virtual ~Notifier() { }
        };

        /**
         * Tracks locking statistics.  For now, just aggregated across all resources/TxIds
         * Eventually might keep per TxId and/or per Resource, to facilitate identifying
         * hotspots and problem transactions.
         */
        class LockStats {
        public:
            LockStats( )
                : _numRequests(0),
                  _numPreexistingRequests(0),
                  _numBlocks(0),
                  _numDeadlocks(0),
                  _numDowngrades(0),
                  _numUpgrades(0),
                  _numMillisBlocked(0) { }

            LockStats(const LockStats& other);
            LockStats& operator=(const LockStats& other);

            void incRequests() { _numRequests++; }
            void incPreexisting() { _numPreexistingRequests++; }
            void incBlocks() { _numBlocks++; }
            void incDeadlocks() { _numDeadlocks++; }
            void incDowngrades() { _numDowngrades++; }
            void incUpgrades() { _numUpgrades++; }
            void incTimeBlocked( unsigned long long numMillis ) { _numMillisBlocked += numMillis; }

            unsigned long long getNumRequests() const { return _numRequests; }
            unsigned long long getNumPreexistingRequests() const { return _numPreexistingRequests; }
            unsigned long long getNumBlocks() const { return _numBlocks; }
            unsigned long long getNumDeadlocks() const { return _numDeadlocks; }
            unsigned long long getNumDowngrades() const { return _numDowngrades; }
            unsigned long long getNumUpgrades() const { return _numUpgrades; }
            unsigned long long getNumMillisBlocked() const { return _numMillisBlocked; }

        private:
            unsigned long long _numRequests;
            unsigned long long _numPreexistingRequests;
            unsigned long long _numBlocks;
            unsigned long long _numDeadlocks;
            unsigned long long _numDowngrades;
            unsigned long long _numUpgrades;
            unsigned long long _numMillisBlocked;
        };

        /**
         * Singleton-factory - retrieves a common instance of LockMgr.
         * Most callers should use this interface for getting an instance
         */
        static LockMgr* getSingleton(const LockingPolicy& policy=FIRST_COME);

        /**
         * It's possibly useful to allow multiple LockMgrs for non-overlapping sets
         * of resources, so the constructor is left public.  But most callers should
         * use the singleton factory method above to get the one and only LockMgr
         */
        LockMgr(const LockingPolicy& policy=FIRST_COME);
        virtual ~LockMgr();

        /**
         * Change the current policy, typically used to temporarily
         * block all readers, writers, or any new resource acquisition
         */
        virtual void setPolicy(const LockingPolicy& policy);

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
        virtual void setParent( const ResourceId& container, const ResourceId& parent );


        /**
         * override default LockMgr's default LockingPolicy for a transaction
         *
         * positive priority moves transaction's resource requests toward the front
         * of the queue, behind only those requests with higher priority
         *
         * negative priority moves transaction's resource requests toward the back
         * of the queue, ahead of only those requests with lower priority.
         *
         * zero priority uses the LockMgr's default LockingPolicy
         */
        void setTransactionPriority( const TxId& xid, int priority );
        int  getTransactionPriority( const TxId& xid );


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
         */
        virtual LockId acquire( const TxId& requestor,
				const unsigned& mode,
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
         * and so on.  If modes is shorter thant resIdPath, the last mode will
         * be used for any remaining ancestor locking.
         */
        virtual LockId acquire( const TxId& requestor,
				const std::vector<unsigned>& modes,
				const std::vector<ResourceId>& resIdPath,
				Notifier* notifier = NULL);

        /**
         * for bulk operations:
         * acquire one of a vector of ResourceIds in a mode,
         * hopefully without blocking, return index of 
         * acquired ResourceId, or -1 if vector was empty
         */
        virtual int acquireOne( const TxId& requestor,
                                const unsigned& mode,
                                const ResourceId& container,
                                const vector<ResourceId>& records,
                                Notifier* notifier = NULL );

        /**
         * release a ResourceId in a container.
         * The mode here is just the mode that applies to the resId
         * The modes that applied to the ancestor containers are
         * already known
         */
        virtual LockStatus release( const TxId& holder,
				    const unsigned& mode,
				    const ResourceId& container,
				    const ResourceId& resId);

	/**
	 * releases the lock returned by acquire.  should perhaps replace above?
	 */
	virtual LockStatus releaseLock( const LockId& lid );

        /**
         * release all resources acquired by a transaction
         * returns number of locks released
         */
        virtual size_t release( const TxId& holder);

        /**
        * called internally for deadlock
        * possibly called publicly to stop a long transaction
        * also used for testing
        */
        void abort( const TxId& goner );

        void getStats( LockStats* stats );



        // --- for testing and logging

         std::string toString( );

        /**
         * test whether a TxId has locked a nested ResourceId in a mode
         */
        virtual bool isLocked( const TxId& holder,
                               const unsigned& mode,
                               const ResourceId& parentId,
                               const ResourceId& resId);

    private:

        /**
         * returns true if a newRequest should be honored before an oldRequest according
         * to the lockManager's policy.  Used by acquire to decide whether a new share request
         * conflicts with a previous upgrade-to-exclusive request that is blocked.
         */
        bool comesBeforeUsingPolicy( const TxId& newReqXid,
                                     const unsigned& newReqMode,
                                     const LockRequest* oldReq );

        /**
         * if a lock request would conflict with others on a queue, set the blocker ouput parameter
         * to the TxId of the first conflicting request, and return true. otherwise return false
         */
        bool conflictExists( const LockRequest* lr, const list<LockId>* queue, TxId* blocker );

        /**
         * returns true if acquire would return without waiting
         * used by acquireOne
         */
        bool isAvailable( const TxId& requestor,
                          const unsigned& mode,
                          const ResourceId& parentId,
                          const ResourceId& resId );

        LockStatus find_lock( const TxId& requestor,
                              const unsigned& mode,
                              const ResourceId& parentId,
                              const ResourceId& resId,
			      LockId* outLid );

        /**
         * set up for future deadlock detection, called from acquire
         */
        void addWaiter( const TxId& blocker, const TxId& waiter );

        /**
         * when inserting a new lock request into the middle of a queue,
         * add any remaining incompatible requests in the queue to the
         * new lock request's set of waiters... for future deadlock detection
         */
        void addWaiters( LockRequest* blocker,
                         list<LockId>::iterator nextLockId,
                         list<LockId>::iterator lastLockId );
        /**
         * adds a lock request to the list of requests for a resource
         * using the LockingPolicy.  Called by acquire
         */
        void addLockToQueueUsingPolicy( LockRequest* lr );

        LockId acquire_internal( const TxId& requestor,
				 const unsigned& mode,
				 const LockId& containerLid,
				 const ResourceId& resId,
				 Notifier* notifier,
                                 boost::unique_lock<boost::mutex>& guard);

        /**
         * called by public ::release and internally by abort.
         * assumes caller as acquired a mutex.
         */
        LockStatus release_internal( const LockId& lid );

        /**
         * called by public ::abort and internally by deadlock
         */
        void abort_internal( const TxId& goner );

        /**
         * called externally by getTransactionPriority
         * and internally by addLockToQueueUsingPolicy
         */
        int get_transaction_priority_internal( const TxId& xid );

        // Singleton instance
        static LockMgr* _singleton;

        // The LockingPolicy controls which requests should be honored first.  This is
        // used to guide the position of a request in a list of requests waiting for
        // a resource or resource container.
        //
        // XXX At some point, we may want this to also guide the decision of which
        // transaction to abort in case of deadlock.  For now, the transaction whose
        // request would lead to a deadlock is aborted.  Since deadlocks are rare,
        // careful choices may not matter much.
        //
        LockingPolicy _policy;

        // synchronizes access to the lock manager, which is shared across threads
        boost::mutex _guard;

        // owns the LockRequest*
        std::map<LockId,LockRequest*> _locks;

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

        // For cleanup and abort processing, contains all LockRequests made by a transaction
        std::map<TxId, std::set<LockId>*> _xaLocks;

        // For deadlock detection: the set of transactions blocked by another transaction
        std::map<TxId, std::set<TxId>*> _waiters;

        // track transactions that have aborted, and don't accept further 
        // lock requests from them (which shouldn't happen anyway).
        //
        std::set<TxId> _abortedTxIds;

        // transaction priorities:
        //     0 => neutral, use LockMgr's default _policy
        //     + => high, queue forward
        //     - => low, queue back
        //
        std::map<TxId, int> _txPriorities;

        // stats
        LockStats _stats;
    };

    /**
     * RAII wrapper around LockMgr, for scoped locking
     */
    class ResourceLock {
    public:
        ResourceLock( LockMgr* lm,
                      const TxId& requestor,
                      const unsigned& mode,
                      const ResourceId& parentId,
                      const ResourceId& resId,
                      LockMgr::Notifier* notifier = NULL );

        virtual ~ResourceLock( );
    private:
	// not owned here
	LockMgr* _lm;
	LockMgr::LockId _lid;
    };
}
