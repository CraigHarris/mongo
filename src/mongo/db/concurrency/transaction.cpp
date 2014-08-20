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
using std::multiset;
using std::string;
using std::stringstream;


namespace mongo {

    const char* Transaction::AbortException::what() const throw() { return "AbortException"; }

    Transaction::Transaction(unsigned txId, int priority)
        : _txId(txId)
        , _scopeLevel(0)
        , _priority(priority)
        , _locks(NULL) { }

    Transaction::~Transaction() {
        invariant(NULL == _locks);
    }

    Transaction* Transaction::setTxIdOnce(unsigned txId) {
        if (0 == _txId) {
            _txId = txId;
        }

        return this;
    }

    void Transaction::enterScope() {
        ++_scopeLevel;
    }

    void Transaction::exitScope(LockManager* lm) {
        invariant(_scopeLevel);
        if (--_scopeLevel > 0) return;

        // relinquish locks acquiredInScope that have been released; and
        // complain about locks acquiredInScope that have not been released

        if (NULL == lm) {
            lm = &LockManager::getSingleton();
        }
        lm->relinquishScopedTxLocks(_locks);
    }

    void Transaction::releaseLocks(LockManager* lm) {
        for (LockRequest* nextLock = _locks; nextLock;) {
            LockRequest* newNextLock = nextLock->nextOfTransaction;
            LockManager::LockStatus status;
            do {
                status = lm->releaseLock(nextLock);
            } while (LockManager::kLockCountDecremented == status);
            nextLock = newNextLock;
        }
        removeAllWaiters();
    }

    void Transaction::abort() {
        removeAllWaiters();
        throw AbortException();
    }

    bool Transaction::operator<(const Transaction& other) const {
        return _txId < other._txId;
    }

    int Transaction::getPriority() const {
        boost::recursive_mutex::scoped_lock lk(_txMutex);
        return _priority;
    }

    void Transaction::setPriority(int newPriority) {
        boost::recursive_mutex::scoped_lock lk(_txMutex);
        _priority = newPriority;
    }

    void Transaction::addLock(LockRequest* lr) {
        lr->nextOfTransaction = _locks;

        if (_locks) {
            _locks->prevOfTransaction = lr;
        }
        _locks = lr;
    }

    void Transaction::removeLock(LockRequest* lr) {
        if (lr->nextOfTransaction) {
            lr->nextOfTransaction->prevOfTransaction = lr->prevOfTransaction;
        }

        if (lr->prevOfTransaction) {
            lr->prevOfTransaction->nextOfTransaction = lr->nextOfTransaction;
        }
        else {
            boost::recursive_mutex::scoped_lock lk(_txMutex);
            _locks = lr->nextOfTransaction;
        }

        lr->nextOfTransaction = NULL;
        lr->prevOfTransaction = NULL;
        if (lr->heapAllocated) delete lr;
    }

    void Transaction::addWaiter(Transaction* waiter) {
        boost::recursive_mutex::scoped_lock lk(_txMutex);
        _waiters.insert(waiter);
        _waiters.insert(waiter->_waiters.begin(), waiter->_waiters.end());
    }

    bool Transaction::hasWaiter(const Transaction* other) const {
        boost::recursive_mutex::scoped_lock lk(_txMutex);
        return _waiters.find(other) != _waiters.end();
    }

    size_t Transaction::numWaiters() const {
        boost::recursive_mutex::scoped_lock lk(_txMutex);
        return _waiters.size();
    }

    void Transaction::removeAllWaiters() {
        boost::recursive_mutex::scoped_lock lk(_txMutex);
        _waiters.clear();
    }

    void Transaction::removeWaiterAndItsWaiters(const Transaction* other) {
        boost::recursive_mutex::scoped_lock lk(_txMutex);
        multiset<const Transaction*>::iterator otherWaiter = _waiters.find(other);
        if (otherWaiter == _waiters.end()) return;
        _waiters.erase(_waiters.find(other));
        boost::recursive_mutex::scoped_lock other_lk(other->_txMutex);
        multiset<const Transaction*>::iterator nextOtherWaiters = other->_waiters.begin();
        for(; nextOtherWaiters != other->_waiters.end(); ++nextOtherWaiters) {
            _waiters.erase(*nextOtherWaiters);
        }
    }

    string Transaction::toString() const {
        stringstream result;
        result << "<xid:" << _txId
               << ",scopeLevel:" << _scopeLevel
               << ",priority:" << _priority;


        result << ",locks: {";
        bool firstLock=true;
        boost::recursive_mutex::scoped_lock lk(_txMutex);
        for (LockRequest* nextLock = _locks; nextLock; nextLock=nextLock->nextOfTransaction) {
            if (firstLock) firstLock=false;
            else result << ",";
            result << nextLock->toString();
        }
        result << "}";

        result << ">,waiters: {";
        bool firstWaiter=true;
        for (multiset<const Transaction*>::const_iterator nextWaiter = _waiters.begin();
             nextWaiter != _waiters.end(); ++nextWaiter) {
            if (firstWaiter) firstWaiter=false;
            else result << ",";
            result << (*nextWaiter)->_txId;
        }
        result << "}>";
        return result.str();
    }

    void Transaction::wait(boost::unique_lock<boost::mutex>& guard) {
        _condvar.wait(guard);
    }

    void Transaction::wake() {
        _condvar.notify_one();
    }

} // namespace mongo
