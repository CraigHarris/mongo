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

#include "mongo/platform/compiler.h"
#include "mongo/platform/cstdint.h"

#include "mongo/base/string_data.h"

namespace mongo {

    class ResourceId {
    public:
        ResourceId() : _rid(0 /* should be kReservedResourceId */) { }

        /**
         * Construct resource identifiers without regard to their scope.  Used for
         * top-level resources (global locks), or when collisions (same resource id
         * allocated by multiple RecordStores) are unlikely.
         */
        ResourceId(uint64_t rid) : _rid(rid) { }
        explicit ResourceId(const void* loc) : _rid(reinterpret_cast<uint64_t>(loc)) { }

        /**
         * Construct resource identifiers taking into account their scope.
         * The native mongo record store allocates the same recordId for the first document
         * in every collection. These constructors avoid locking skew in such cases.
         */
        ResourceId(uint64_t resourceId, const StringData& scope);
        ResourceId(uint64_t resourceId, const void* resourceIdAllocator);

        bool operator<(const ResourceId& other) const { return _rid < other._rid; }
        bool operator==(const ResourceId& other) const { return _rid == other._rid; }
        operator uint64_t() const { return _rid; }

    private:
        uint64_t _rid;
    };
    static const ResourceId kReservedResourceId;
} // namespace mongo
