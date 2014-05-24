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
    typedef size_t TxId;
    typedef size_t RecordId;

    class AbortException : public std::exception {
    public:
        AbortException() {}
        const char* what() const throw () { return "AbortException"; }
    };

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
            BIGGEST_BLOCKER_FIRST,
            QUICKEST_TO_FINISH
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
	    bool sleep;
            LockId lid;
            TxId xid;
            LockMode mode;
            const RecordStore* store;
            RecordId recId;
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
         * release a RecordStore
         */
        virtual void release( const TxId& holder,
                              const LockMode& mode,
                              const RecordStore* store);

        /**
         * release a RecordId in a RecordStore
         */
        virtual void release( const TxId& holder,
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

        const LockStats& getStats( );

	std::string toString( ) const;


        // --- for testing and logging
#if 0
        /**
         * iterate over locks held by TxId
         */
        virtual std::iterator<LockRequest*> begin(const TxId& xa);

        /**
         * iterate over locks on a given store
         */
        virtual std::iterator<LockRequest*> begin(const RecordStore* store);

        /**
         * iterate over locks on a given record
         */
        virtual std::iterator<LockRequest*> begin(const RecordStore* store, const RecordId& recId);
#endif
    private:

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
         * called by public release and internally by abort
         * assumes caller as acquired a mutex
         */
        virtual void releaseWithMutex( const TxId& holder,
                                       const LockMode& mode,
                                       const RecordStore* store);

        virtual void releaseWithMutex( const TxId& holder,
                                       const LockMode& mode,
                                       const RecordStore* store,
                                       const RecordId& recId);

        LockingPolicy _policy;

        boost::mutex _guard;

        // owns the LockRequest*
        std::map<LockId,LockRequest*> _locks;

        std::map<const RecordStore*, std::map<RecordId, std::list<LockId>*> > _recordLocks;
        std::map<const RecordStore*, std::list<LockId>*> _containerLocks;

        // for cleanup and abort processing
        std::map<TxId, std::set<LockId>*> _xaLocks;

        // for deadlock detection
        std::map<TxId, std::set<TxId>*> _waiters;

        // stats
        LockStats _stats;
    };
}
