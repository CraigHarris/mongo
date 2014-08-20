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

namespace mongo {

    /**
     * LockModes are used to control access to resources
     *
     *    kIntentShared: acquired on containers to indicate intention to acquire
     *                   shared lock on contents.  Prevents container from being
     *                   modified while working with contents. should be acquired
     *                   on databases and collections before accessing documents
     *
     *    kIntentExclusive: similar to above, prevents container from being locked
     *                      in a way that would block exclusive locks on contents
     *                      should be acquired on databases and collections before
     *                      modifying their content.
     *
     *    kShared: acquired on either containers or leaf resources before viewing
     *             their state.  prevents concurrent modifications to their state
     *
     *    kSIX: not sure we have a use case for this
     *
     *    kUpdate: an optimization that reduces deadlock frequency compared to
     *             acquiring kShared and then upgrading to kExclusive.  Acts as
     *             a kShared lock, and must be upgraded to kExclusive before
     *             modifying the locked resource.
     *
     *    kBlockExclusive: acquired on a container to block exclusive access to
     *                     its contents.  If multiple kBlockExclusive locks are
     *                     acquired, exclusive access is blocked from the time
     *                     the first lock is acquired until the last lock is
     *                     released.  Also blocks kUpdate locks
     *
     *    kExclusive: acquired before modifying a resource, blocks all other
     *                concurrent access to the resource.
     *                   
     */

    /**
     * LockMode Compatibility Matrix:
     *                                           Granted Mode
     *                               +-----+-----+-----+-----+-----+-----+-----+
     * Requested Mode                | IS  | IX  |  S  | SIX |  U  | BX  |  X  |
     * --------------                +-----+-----+-----+-----+-----+-----+-----+
     * kIntentShared(IS)             | ok  | ok  | ok  | ok  | ok  | ok  |  -  |
     *                               +-----+-----+-----+-----+-----+-----+-----+
     * kIntentExclusive(IX)          | ok  | ok  |  -  |  -  |  -  |  -  |  -  |
     *                               +-----+-----+-----+-----+-----+-----+-----+
     * kShared(S)                    | ok  |  -  | ok  |  -  | ok  | ok  |  -  |
     *                               +-----+-----+-----+-----+-----+-----+-----+
     * kSharedOrIntentExclusive(SIX) | ok  |  -  |  -  |  -  |  -  |  -  |  -  |
     *                               +-----+-----+-----+-----+-----+-----+-----+
     * kUpdate(U)                    | ok  |  -  | ok  |  -  |  -  |  -  |  -  |
     *                               +-----+-----+-----+-----+-----+-----+-----+
     * kBlockExclusive(BX)           | ok  |  -  | ok  |  -  |  -  | ok  |  -  |
     *                               +-----+-----+-----+-----+-----+-----+-----+
     * kExclusive(X)                 |  -  |  -  |  -  |  -  |  -  |  -  |  -  |
     *                               +-----+-----+-----+-----+-----+-----+-----+
     */

    /**
     * Upgrades:
     *     IS  -> {S, IX}
     *     S   -> {SIX, U}
     *     IX  -> {SIX}
     *     SIX -> X
     *     U   -> X
     *     BX  -> X
     */

    namespace Locking {

        enum LockMode{
            kIntentShared,
            kIS = kIntentShared,

            kIntentExclusive,
            kIX = kIntentExclusive,

            kShared,
            kS = kShared,

            kSharedIntentExclusive,
            kSIX = kSharedIntentExclusive,

            kUpdate,
            kU = kUpdate,

            kBlockExclusive,
            kBX = kBlockExclusive,

            kExclusive,
            kX = kExclusive
        };

        bool conflictsWithReadersOnlyPolicy(const LockMode& mode);
        bool conflictsWithWritersOnlyPolicy(const LockMode& mode);

        /**
         * return true if requested mode would be an upgrade of acquired mode:
         *     IS < S  < SIX < X
         *     IS < S  <  U  < X
         *     IS < IX < SIX < X
         *                SX < X
         */
        bool isUpgrade(const LockMode& acquired, const LockMode& requested);
        bool isDowngrade(const LockMode& acquired, const LockMode& requested);
        bool isCompatible(const LockMode& acquired, const LockMode& requested);
    };
} // namespace mongo
