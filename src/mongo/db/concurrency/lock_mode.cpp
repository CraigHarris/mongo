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

#include "mongo/db/concurrency/lock_mode.h"

#include "mongo/util/assert_util.h"

namespace mongo {

    namespace Locking {

        bool conflictsWithReadersOnlyPolicy(const LockMode& mode) {
            switch (mode) {
            case kIntentExclusive:
            case kExclusive:
                return true;

            case kIntentShared:
            case kShared:
            case kSharedIntentExclusive:
            case kUpdate:
            case kBlockExclusive:
                return false;
            }

            invariant(false); // unreachable
        }

        bool conflictsWithWritersOnlyPolicy(const LockMode& mode) {
            switch (mode) {
            case kIntentShared:
            case kShared:
            case kSharedIntentExclusive:
            case kUpdate:
            case kBlockExclusive:
                return true;

            case kIntentExclusive:
            case kExclusive:
                return false;
            }

            invariant(false); // unreachable
        }

        /**
         * return true if requested mode would be an upgrade of acquired mode:
         *     IS < S  < SIX < X
         *     IS < S  <  U  < X
         *     IS < IX < SIX < X
         *                SX < X
         */
        bool isUpgrade(const LockMode& acquired, const LockMode& requested) {
            switch (acquired) {
            case kIntentShared:
                return kIntentShared != requested; // everything else is an upgrade
            case kShared:
                return
                    (kSharedIntentExclusive == requested) ||
                    (kUpdate == requested) ||
                    (kExclusive == requested);
            case kIntentExclusive:
                return
                    (kSharedIntentExclusive == requested) ||
                    (kExclusive == requested);
            case kSharedIntentExclusive:
            case kUpdate:
            case kBlockExclusive:
                return kExclusive == requested;
            case kExclusive:
                return false; // X is the top of the ladder
            }

            invariant(false); // unreachable
        }

        bool isDowngrade(const LockMode& acquired, const LockMode& requested) {
            switch (acquired) {
            case kIntentShared:
                return false; // nothing lower than kIS
            case kShared:
            case kIntentExclusive:
                return kIntentShared == requested;
            case kSharedIntentExclusive:
                return
                    (kIntentExclusive == requested) ||
                    (kShared == requested) ||
                    (kIntentShared == requested);
            case kUpdate:
                return
                    (kShared == requested) ||
                    (kIntentShared == requested);
            case kBlockExclusive:
                return false;
            case kExclusive:
                return kExclusive != requested; // everything else is lower
            }

            invariant(false); // unreachable
        }

        bool isChildCompatible(const LockMode& parentMode, const LockMode& childMode) {
            if (parentMode == childMode) return true;

            // childMode not 
            switch (parentMode) {

            case kIntentShared:
                return kShared == childMode; // otherwise, upgrade lock on parent

            case kIntentExclusive:
            case kSharedIntentExclusive: 
                return true;

            case kUpdate: // some ancestor of parentMode must have been kIX to get here
                return (kShared == childMode) || (kExclusive == childMode);

            case kBlockExclusive:
                return (kIntentShared == childMode) || (kShared == childMode);

            case kShared:
            case kExclusive:
                return false;
            }

            invariant(false); // unreachable
        }

        /**
         * return false if requested conflicts with acquired, true otherwise
         */
        bool isCompatible(const LockMode& acquired, const LockMode& requested) {
            switch (acquired) {
            case kIntentShared:
                return kExclusive != requested;
            case kIntentExclusive:
                switch (requested) {
                case kIntentShared:
                case kIntentExclusive:
                    return true;
                default:
                    return false;
                }
            case kShared:
                switch (requested) {
                case kIntentShared:
                case kShared:
                case kUpdate:
                case kBlockExclusive:
                    return true;
                default:
                    return false;
                }
            case kSharedIntentExclusive:
                return kIntentShared == requested;
            case kUpdate:
                switch (requested) {
                case kIntentShared:
                case kShared:
                    return true;
                default:
                    return false;
                }
            case kBlockExclusive:
                switch (requested) {
                case kShared:
                case kIntentShared:
                case kBlockExclusive:
                    return true;
                default:
                    return false;
                }
            case kExclusive:
            default:
                return false;
            }

            invariant(false); // unreachable
        }

    } // namespace Locking

} // namespace mongo
