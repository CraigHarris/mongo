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
 * Resources are either RecordStores, or Records within an RecordStore, identified by a RecordId.
 * Resources are acquired for either shared or exclusive use, by transactions identified by a TxId.
 * Acquiring Records in any mode implies acquisition of the Record's RecordStore
 *
 * Contention for a resource is resolved by a LockingPolicy, which determines which blocked
 * resource requests to awaken when the blocker releases the resource.
 *
 */

namespace mongo {
    typedef size_t TxId;        // identifies requesting transaction
    typedef size_t RecordId;
    typedef size_t ResourceId;  // identifies requested resource
    typedef size_t Containerid; // identifies container of requested resource

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
     *     acquire - a transaction acquires a resource for shared or exclusive use
     *     release - a transaction releases a resource previously acquired for shared/exclusive use
     */

    class LockMgr {
    public:

        enum LockMode {
            SHARED_RECORD,
            SHARED_STORE,
            EXCLUSIVE_RECORD,
            EXCLUSIVE_STORE,
            INVALID
        };

        enum LockingPolicy {
            FIRST_COME,
            READERS_FIRST,
            OLDEST_TX_FIRST,
            BIGGEST_BLOCKER_FIRST
        };

        enum ReleaseStatus {
            RELEASED,
            COUNT_DECREMENTED,
            CONTAINER_NOT_ACQUIRED,
            RESOURCE_NOT_ACQUIRED,
            RESOURCE_NOT_ACQUIRED_IN_MODE
        };

        typedef size_t LockId;

        class LockRequest {
        public:
            LockRequest( const TxId& xid,
                         const LockMode& mode,
                         const RecordStore* store);

            LockRequest( const TxId& xid,
                         const LockMode& mode,
                         const RecordStore* store,
                         const RecordId& recId );

            virtual ~LockRequest();

            bool matches( const TxId& xid,
                          const LockMode& mode,
                          const RecordStore* store,
                          const RecordId& recId );

            std::string toString( ) const;

            static LockId nextLid;
            bool sleep;                // true if waiting, false if resource acquired
            LockId lid; 
            TxId xid;                  // transaction that made this request
            LockMode mode;             // shared or exclusive use
            const RecordStore* store;  // container of resource requested
            RecordId recId;            // resource requested
            size_t count;              // # times xid requested this resource in this mode
                                       // request will be deleted when count goes to 0
            boost::condition_variable lock;
        };

        class Notifier {
        public:
            virtual void operator()(const TxId& blocker) = 0;

            virtual ~Notifier() { }
        };

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

        LockMgr(const LockingPolicy& policy=FIRST_COME);
        virtual ~LockMgr();

        /**
         * acquire a RecordStore in a mode.
         * can throw AbortException
         */
        virtual void acquire( const TxId& requestor,
                              const LockMode& mode,
                              const RecordStore* store );

        /**
         * acquire a RecordId in a RecordStore in a mode
         * can throw AbortException
         */
        virtual void acquire( const TxId& requestor,
                              const LockMode& mode,
                              const RecordStore* store,
                              const RecordId& recId,
                              Notifier* notifier = NULL);

        /**
         * for bulk operations:
         * acquire one of a vector of RecordIds in a mode,
         * hopefully without blocking, return index of 
         * acquired RecordId, or -1 if vector was empty
         */
        virtual int acquireOne( const TxId& requestor,
                                const LockMode& mode,
                                const RecordStore* store,
                                const vector<RecordId>& records,
                                Notifier* notifier = NULL );

        /**
         * release a RecordStore
         */
        virtual void release( const TxId& holder,
                              const LockMode& mode,
                              const RecordStore* store);

        /**
         * release a RecordId in a RecordStore
         */
        virtual ReleaseStatus release( const TxId& holder,
                                       const LockMode& mode,
                                       const RecordStore* store,
                                       const RecordId& recId);

        /**
         * release all resources acquired by a transaction
         */
        virtual size_t release( const TxId& holder);

        /**
        * called internally for deadlock
        * possibly called publicly to stop a long transaction
        * also used for testing
        */
        void abort( const TxId& goner );

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
        int  getTransactionPriority( const TxId& xid ) const;

        const LockStats& getStats( );



        // --- for testing and logging

         std::string toString( ) const;

        /**
         * test whether a TxId has locked RecordStore in a mode
         */
        virtual bool isLocked( const TxId& holder,
                               const LockMode& mode,
                               const RecordStore* store);

        /**
         * test whether a TxId has locked a RecordId in a mode
         */
        virtual bool isLocked( const TxId& holder,
                               const LockMode& mode,
                               const RecordStore* store,
                               const RecordId& recId);

    private:

        /**
         * returns true if a newRequest should be honored before an oldRequest according
         * to the lockManager's policy.  Used by acquire to decide whether a new share request
         * conflicts with a previous upgrade-to-exclusive request that is blocked.
         */
        bool comesBeforeUsingPolicy( const TxId& newReqXid,
                                     const LockMode& newReqMode,
                                     const LockRequest* oldReq ) const;

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
                          const LockMode& mode,
                          const RecordStore* store,
                          const RecordId& recId );

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

        /**
         * called by public release and internally by abort.
         * assumes caller as acquired a mutex.
         */
        virtual void releaseWithMutex( const TxId& holder,
                                       const LockMode& mode,
                                       const RecordStore* store);

        virtual ReleaseStatus releaseWithMutex( const TxId& holder,
                                                const LockMode& mode,
                                                const RecordStore* store,
                                                const RecordId& recId);

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

        // Lists of lock requests associated with a resource or container.
        //
        // The containing lists have two sections.  Some number (at least one) of requests 
        // at the front of a list are "active".  All remaining lock requests are blocked by
        // some earlier (not necessarily active) lock request, and are waiting.  The order
        // of lock request in the waiting section is determined by the LockPolicty.
        // The order of lock request in the active/front portion of the list is irrelevant.
        //
        std::map<const RecordStore*, std::map<RecordId, std::list<LockId>*> > _recordLocks;
        std::map<const RecordStore*, std::list<LockId>*> _containerLocks;

        // For cleanup and abort processing, all LockRequests made by a transaction
        std::map<TxId, std::set<LockId>*> _xaLocks;

        // For deadlock detection: the set of transactions blocked by another transaction
        // 
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
                      const LockMgr::LockMode& mode,
                      const RecordStore* store,
                      const RecordId& recId,
                      LockMgr::Notifier* notifier = NULL );

        virtual ~ResourceLock( );
    private:
        LockMgr* _lm;   // not owned here
        TxId _requestor;
        LockMgr::LockMode _mode;
        const RecordStore* _store; // not owned here
        RecordId _recId;
    };
}
