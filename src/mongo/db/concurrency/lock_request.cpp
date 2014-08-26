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

#include "mongo/db/concurrency/lock_request.h"

#include "mongo/db/concurrency/lock_mgr.h"
#include "mongo/db/concurrency/lock_mode.h"
#include "mongo/db/concurrency/resource_id.h"
#include "mongo/db/concurrency/transaction.h"

#include <sstream>

#include "mongo/base/init.h"
#include "mongo/util/assert_util.h"

using std::endl;
using std::string;
using std::stringstream;

namespace mongo {

    using namespace Locking;

    LockRequest::LockRequest(const ResourceId& resId,
                             const LockMode& mode,
                             Transaction* tx,
                             bool heapAllocated)
        : requestor(tx)
        , mode(mode)
        , resId(resId)
        , slice(LockManager::partitionResource(resId))
        , acquiredInScope(tx->inScope())
        , count(1)
        , sleepCount(0)
        , heapAllocated(heapAllocated)
        , nextOnResource(NULL)
        , prevOnResource(NULL)
        , nextOfTransaction(NULL)
        , prevOfTransaction(NULL) { }


    LockRequest::~LockRequest() {
        verify(NULL == nextOfTransaction);
        verify(NULL == prevOfTransaction);
        verify(NULL == nextOnResource);
        verify(NULL == prevOnResource);
    }

    bool LockRequest::matches(const Transaction* tx,
                              const LockMode& mode,
                              const ResourceId& resId) const {
        return
            this->requestor == tx &&
            this->mode == mode &&
            this->resId == resId;
    }

    string LockRequest::toString() const {
        stringstream result;
        result << "<xid:" << requestor->getTxId()
               << ",mode:" << mode
               << ",resId:" << resId.toString()
               << ",count:" << count
               << ",sleepCount:" << sleepCount
               << ">";
        return result.str();
    }

    bool LockRequest::isBlocked() const {
        return sleepCount > 0;
    }

    bool LockRequest::shouldAwake() {
        // no evidence of underflow, but protect here
        if (0 == sleepCount) return true;

        return 0 == --sleepCount;
    }

    void LockRequest::insert(LockRequest* lr) {
        lr->prevOnResource = this->prevOnResource;
        lr->nextOnResource = this;

        if (this->prevOnResource) {
            this->prevOnResource->nextOnResource = lr;
        }
        this->prevOnResource = lr;
    }

    void LockRequest::append(LockRequest* lr) {
        lr->prevOnResource = this;
        lr->nextOnResource = this->nextOnResource;

        if (this->nextOnResource) {
            this->nextOnResource->prevOnResource = lr;
        }
        this->nextOnResource = lr;
    }

} // namespace mongo
