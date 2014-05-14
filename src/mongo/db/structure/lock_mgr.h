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
#include <iterator>
#include <list>
#include <map>
#include <vector>
#include <set>

#include "mongo/db/structure/record_store.h"
#include "mongo/util/concurrency/mutex.h"

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

            static LockId nextLid;
            LockId lid;
            TxId xid;
            LockMode mode;
            const RecordStore* store;
            RecordId recId;
            boost::condition_variable lock;
        };

        LockMgr(const LockingPolicy& policy=FIRST_COME);
        virtual ~LockMgr() = 0;

        /*
         * acquire a RecordStore in a mode
         */
        virtual void acquire( const TxId& requestor,
                              const LockMode& mode,
                              const RecordStore& store ) = 0;

        /*
         * acquire a RecordId in a RecordStore in a mode
         */
        virtual void acquire( const TxId& requestor,
                              const LockMode& mode,
                              const RecordStore& store,
                              const RecordId& recId ) = 0;

        /*
         * acquire several RecordIds in a RecordStore in a mode
         */
        virtual void acquire( const TxId& requestor,
                              const LockMode& mode,
                              const RecordStore& store,
                              const std::vector<RecordId>& recordIds ) = 0;

        /*
         * release a RecordStore
         */
        virtual void release( const TxId& holder,
                              const LockMode& mode,
                              const RecordStore& store) = 0;

        /*
         * release a RecordId in a RecordStore
         */
        virtual void release( const TxId& holder,
                              const LockMode& mode,
                              const RecordStore& store,
                              const RecordId& recId) = 0;

        /*
         * release a several RecordIds in a RecordStore
         */
        virtual void release( const TxId& holder,
                              const LockMode& mode,
                              const RecordStore& store,
                              const vector<RecordId>& recordIds) = 0;

        /*
         * release all resources acquired by a transaction
         */
        virtual void release( const TxId& holder) = 0;


        // --- for testing and logging
#if 0
        /**
         * iterate over locks held by TxId
         */
        virtual std::iterator<LockRequest*> begin(const TxId& xa) = 0;

        /**
         * iterate over locks on a given store
         */
        virtual std::iterator<LockRequest*> begin(const RecordStore* store) = 0;

        /**
         * iterate over locks on a given record
         */
        virtual std::iterator<LockRequest*> begin(const RecordStore* store, const RecordId& recId) = 0;
#endif
    private:
        void addLockToQueueUsingPolicy( LockRequest* lr );
        bool detectDeadlock(const TxId& acquirer, TxId* goner);
        void abort(const TxId& goner);

        LockingPolicy _policy;

        mongo::mutex _guard;

        // owns the LockRequest*
        std::map<LockId,LockRequest*> _locks;

        std::map<RecordStore, std::map<RecordId, std::list<LockId>*> > _recordLocks;
        std::map<RecordStore, std::list<LockId>*> _containerLocks;

        // for cleanup and abort processing
        std::map<TxId, std::set<LockId>*> _xaLocks;

        // for deadlock detection
        std::map<TxId, std::set<TxId>*> _waiters;
    };
}
