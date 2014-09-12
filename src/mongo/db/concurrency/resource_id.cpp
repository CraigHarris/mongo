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
#include "mongo/db/concurrency/resource_id.h"

#include <boost/functional/hash.hpp>
#include <sstream>

#include "mongo/base/init.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/mongoutils/str.h"

using std::endl;
using std::string;
using std::stringstream;

namespace mongo {

    ResourceId::ResourceId(uint64_t resourceId, const StringData& scope) {
        StringData::Hasher hasher;
        size_t result = 0;
        boost::hash_combine(result, hasher(scope));
        boost::hash_combine(result, resourceId);
        _rid=result;
    }

    ResourceId::ResourceId(uint64_t resourceId, const void* resourceIdAllocator) {
        size_t result = 0;
//        uint64_t allocatorId = reinterpret_cast<uint64_t>(resourceIdAllocator);
        boost::hash_combine(result, resourceIdAllocator);
        boost::hash_combine(result, resourceId);
        _rid = result;
    }

    uint64_t ResourceId::hash() const {
        // when resIds are DiskLocs, their low-order bits are mostly zero
        // so add up nibbles as cheap hash
        uint64_t resIdValue = _rid;
        size_t resIdHash = 0;
        size_t mask = 0xf;
        for (unsigned ix=0; ix < 2*sizeof(uint64_t); ++ix) {
            resIdHash += (resIdValue >> ix*4) & mask;
        }
        return resIdHash;
    }

    string ResourceId::toString() const {
        return str::stream() << _rid;
    }

} // namespace mongo