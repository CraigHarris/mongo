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
#include <set>

#include "mongo/platform/compiler.h"
#include "mongo/platform/cstdint.h"
#include "mongo/base/string_data.h"
#include "mongo/util/timer.h"

#include "mongo/db/concurrency/lock_mgr.h"

namespace mongo {

    class LockRequest;

    /**
     * Data structure used to describe resource requestors,
     * used for conflict resolution, deadlock detection, and abort
     */
    class Transaction {
    public:

        Transaction(LockManager& lm, unsigned txId, int priority=0);
        ~Transaction();

        /**
         * transactions are identified by an id
         */
        unsigned getTxId() const { return _txId; }

        /**
         * scope corresponds to UnitOfWork nesting level, but may apply to
         * read transactions of 3rd-party storage engines.
         */
        bool inScope() const { return _scopeLevel != 0; }
        void enterScope();
        void exitScope();

        void releaseLocks();

        bool isReader() const { return _readerOnly; }

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
         * @return true if adding waiter would deadlock
         * otherwise add waiter and its waiters to this transaction
         */
        bool addWaiter(Transaction* waiter);
        size_t numWaiters() const;
        void removeWaiterAndItsWaiters(Transaction* other);
        void removeAllWaiters();

        /**
         * if @end is a member of this transaction's waiters,
         * @return the subset of this transaction's waiters that also
         * contain @end in their set of waiters.  The result is the
         * set of Transactions that would form a cycle
         *
         * used by LockManager to choose a transaction to abort.
         */
        std::set<Transaction*> getCycleMembers(Transaction* end);

        /**
         * for sleeping and waking
         */
        void wait(boost::unique_lock<boost::mutex>& guard);
        void wake();


        /**
         * when LockManager decides to abort a transaction that is blocked
         * it calls rememberToAbort and then notifies/wakes the blocked Tx.
         */
        void rememberToAbort() { _shouldAbort = true; }
        bool shouldAbort() {
            if (_shouldAbort) {
                _shouldAbort = false;
                return true;
            }
            return false;
        }

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

        // identify the lock manager
        LockManager& _lm;

        // identify the transaction
        unsigned _txId;

        // 0 ==> outside unit of work.  Used to control relinquishing locks
        size_t _scopeLevel;

        // When the LockManager chooses to abort a transaction that is blocked
        // it sets _shouldAbort and notifies/wakes the transaction, which
        // examines this flag before proceeding
        bool _shouldAbort;

        // when deciding which transaction in a dependency cycle to abort
        // favor readers, and choose among those who have reqeuested something
        // other than IntentShared and Shared locks
        bool _readerOnly;

        // transaction priorities:
        //     0 => neutral, use LockManager's default _policy
        //     + => high, queue forward
        //     - => low, queue back
        //
        AtomicInt32 _priority;

        // synchronize access to transaction's _locks and _waiters
        // which are modified by the lock manager
        mutable boost::mutex _txMutex;

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
        std::multiset<Transaction*> _waiters;
    };
} // namespace mongo
