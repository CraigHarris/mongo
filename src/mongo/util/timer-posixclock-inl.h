/*    Copyright 2010 10gen Inc.
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
 *    must comply with the GNU Affero General Public License in all respects
 *    for all of the code used other than as permitted herein. If you modify
 *    file(s) with this exception, you may extend this exception to your
 *    version of the file(s), but you are not obligated to do so. If you do not
 *    wish to do so, delete this exception statement from your version. If you
 *    delete this exception statement from all source files in the program,
 *    then also delete it in the license file.
 */

/**
 * Inline function implementations for timers on systems that support the
 * POSIX clock API and CLOCK_MONOTONIC clock.
 *
 * This file should only be included through timer-inl.h, which selects the
 * particular implementation based on target platform.
 */

#define MONGO_TIMER_IMPL_POSIX_MONOTONIC_CLOCK

#include <ctime>

#include "mongo/util/assert_util.h"

namespace mongo {

    unsigned long long Timer::now() const {
        timespec the_time;
        unsigned long long result;

        fassert(16160, !clock_gettime(CLOCK_MONOTONIC, &the_time));

        // Safe for 292 years after the clock epoch, even if we switch to a signed time value.  On
        // Linux, the monotonic clock's epoch is the UNIX epoch.
        result = static_cast<unsigned long long>(the_time.tv_sec);
        result *= nanosPerSecond;
        result += static_cast<unsigned long long>(the_time.tv_nsec);
        return result;
    }

}  // namespace mongo
