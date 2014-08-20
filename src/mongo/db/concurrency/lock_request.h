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

#include <string>

#include "mongo/platform/compiler.h"
#include "mongo/platform/cstdint.h"

#include "mongo/db/concurrency/lock_mode.h"
#include "mongo/db/concurrency/resource_id.h"

namespace mongo {

    class Transaction;

    /**
     * Data structure used to record a resource acquisition request
     */
    class LockRequest {
    public:
        LockRequest(const ResourceId& resId,
                    const Locking::LockMode& mode,
                    Transaction* requestor,
                    bool heapAllocated = false);


        ~LockRequest();

        bool matches(const Transaction* tx,
                     const Locking::LockMode& mode,
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
        const Locking::LockMode mode;

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
} // namespace mongo
