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
using std::map;
using std::multiset;
using std::set;
using std::string;
using std::stringstream;
using std::vector;

namespace mongo {

    ResourceId::ResourceId(uint64_t resourceId, const StringData& scope) {
        StringData::Hasher hasher;
        size_t scopeHash = hasher(scope);
        _rid = resourceId ^ scopeHash;
    }

    ResourceId::ResourceId(uint64_t resourceId, const void* resourceIdAllocator) {
        uint64_t allocatorId = reinterpret_cast<uint64_t>(resourceIdAllocator);

        // cantor's pairing function
        _rid = (resourceId+allocatorId)*(resourceId+allocatorId+1)/2 + allocatorId;
    }

} // namespace mongo
