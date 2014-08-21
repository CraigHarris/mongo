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

#include "mongo/db/concurrency/lock_mgr.h"
#include "mongo/db/concurrency/lock_mode.h"
#include "mongo/db/concurrency/lock_request.h"
#include "mongo/db/concurrency/resource_id.h"
#include "mongo/db/operation_context.h"

namespace mongo {

    class OperationContext;

    /**
     * RAII wrapper around LockManager, for scoped locking.
     * subclasses provided for specific locking modes
     */
    class ResourceLock {
    public:
        /**
         * acquire & release lock in given mode on requested resource
         */
        ResourceLock(OperationContext* requestor,
                     const Locking::LockMode& mode,
                     const ResourceId& resId,
                     LockManager::Notifier* notifier = NULL)
            : _lm(LockManager::getSingleton())
            , _lr(resId, mode, requestor->getTransaction())
        {
            _lm.acquireLock(&_lr, notifier);
        }


        /**
         * acquire & release lock in given mode on child resource
         * check that parent resource is locked, and if in matching mode,
         * skip the lock acquisition on the child
         */
        ResourceLock(OperationContext* requestor,
                     const Locking::LockMode& mode,
                     const ResourceId& childResId,
                     const ResourceId& parentResId,
                     LockManager::Notifier* notifier = NULL)
            : _lm(LockManager::getSingleton())
            , _lr(childResId, mode, requestor->getTransaction())
        {
            _lm.acquireLockUnderParent(&_lr, parentResId, notifier);
        }

        ~ResourceLock() {
            _lm.releaseLock(&_lr);
        }
    private:
        LockManager& _lm;
        LockRequest _lr;
    };

    class IntentSharedResourceLock : public ResourceLock {
    public:
        /**
         * acquire & release lock in given mode on requested resource
         */
        IntentSharedResourceLock(OperationContext* requestor,
                                 const ResourceId& resId,
                                 LockManager::Notifier* notifier = NULL)
            : ResourceLock(requestor, Locking::kIntentShared, resId, notifier) { }

        /**
         * acquire & release lock in given mode on child resource
         * check that parent resource is locked, and if in matching mode,
         * skip the lock acquisition on the child
         */
        IntentSharedResourceLock(OperationContext* requestor,
                                 const ResourceId& childResId,
                                 const ResourceId& parentResId,
            LockManager::Notifier* notifier = NULL)
            : ResourceLock(requestor, Locking::kIntentShared, childResId, parentResId, notifier) { }

        ~IntentSharedResourceLock();
    };

    class IntentExclusiveResourceLock : public ResourceLock {
    public:
        /**
         * acquire & release lock in given mode on requested resource
         */
        IntentExclusiveResourceLock(OperationContext* requestor,
                                    const ResourceId& resId,
                                    LockManager::Notifier* notifier = NULL)
            : ResourceLock(requestor, Locking::kIntentExclusive, resId, notifier) { }

        /**
         * acquire & release lock in given mode on child resource
         * check that parent resource is locked, and if in matching mode,
         * skip the lock acquisition on the child
         */
        IntentExclusiveResourceLock(OperationContext* requestor,
                                    const ResourceId& childResId,
                                    const ResourceId& parentResId,
                                    LockManager::Notifier* notifier = NULL)
            : ResourceLock(requestor, Locking::kIntentExclusive, childResId, parentResId, notifier)
        { }

        ~IntentExclusiveResourceLock() { }
    };

    class SharedResourceLock : public ResourceLock {
    public:
        /**
         * acquire & release shared lock on requested resource
         */
        SharedResourceLock(OperationContext* requestor,
                           const ResourceId& resId,
                           LockManager::Notifier* notifier = NULL)
            : ResourceLock(requestor, Locking::kShared, resId, notifier) { }

        /**
         * acquire & release lock shared on child resource
         * check that parent resource is locked, and if also shared
         * skip the lock acquisition on the child
         */
        SharedResourceLock(OperationContext* requestor,
                           const ResourceId& childResId,
                           const ResourceId& parentResId,
                           LockManager::Notifier* notifier = NULL)
            : ResourceLock(requestor, Locking::kShared, childResId, parentResId, notifier) { }

        ~SharedResourceLock() { }
    };

    class UpdateResourceLock : public ResourceLock {
    public:
        /**
         * acquire & release lock update on requested resource
         */
        UpdateResourceLock(OperationContext* requestor,
                           const ResourceId& resId,
                           LockManager::Notifier* notifier = NULL)
            : ResourceLock(requestor, Locking::kUpdate, resId, notifier) { }

        /**
         * acquire & release lock update on child resource
         * check that parent resource is locked, and if also update
         * skip the lock acquisition on the child
         */
        UpdateResourceLock(OperationContext* requestor,
                           const ResourceId& childResId,
                           const ResourceId& parentResId,
                           LockManager::Notifier* notifier = NULL)
            : ResourceLock(requestor, Locking::kUpdate, childResId, parentResId, notifier) { }

        ~UpdateResourceLock() { }
    };

    class SharedIntentExclusiveResourceLock : public ResourceLock {
    public:
        /**
         * acquire and release exlusive lock on requested resource
         */
        SharedIntentExclusiveResourceLock(OperationContext* requestor,
                                          const ResourceId& resId,
                                          LockManager::Notifier* notifier = NULL)
            : ResourceLock(requestor, Locking::kSharedIntentExclusive, resId, notifier) { }

        /**
         * acquire and release exclusive lock on child resource
         * check that parent resource is locked, and if also exclusive
         * skip the lock acquisition on the child
         */
        SharedIntentExclusiveResourceLock(OperationContext* requestor,
                                          const ResourceId& childResId,
                                          const ResourceId& parentResId,
                                          LockManager::Notifier* notifier = NULL)
            : ResourceLock(requestor, Locking::kSharedIntentExclusive,
                           childResId, parentResId, notifier) { }

        ~SharedIntentExclusiveResourceLock() { }
    };

    class BlockExclusiveResourceLock : public ResourceLock {
    public:
        /**
         * acquire & release exlusive lock on requested resource
         */
        BlockExclusiveResourceLock(OperationContext* requestor,
                              const ResourceId& resId,
                              LockManager::Notifier* notifier = NULL)
            : ResourceLock(requestor, Locking::kBlockExclusive,
                           resId, notifier) { }
                

        /**
         * acquire & release exclusive lock on child resource
         * check that parent resource is locked, and if also exclusive
         * skip the lock acquisition on the child
         */
        BlockExclusiveResourceLock(OperationContext* requestor,
                              const ResourceId& childResId,
                              const ResourceId& parentResId,
                              LockManager::Notifier* notifier = NULL)
            : ResourceLock(requestor, Locking::kBlockExclusive, childResId, parentResId, notifier) { }

        ~BlockExclusiveResourceLock() { }
    };

    class ExclusiveResourceLock : public ResourceLock {
    public:
        /**
         * acquire & release exlusive lock on requested resource
         */
        ExclusiveResourceLock(OperationContext* requestor,
                              const ResourceId& resId,
                              LockManager::Notifier* notifier = NULL)
            : ResourceLock(requestor, Locking::kExclusive, resId, notifier) { }
                

        /**
         * acquire & release exclusive lock on child resource
         * check that parent resource is locked, and if also exclusive
         * skip the lock acquisition on the child
         */
        ExclusiveResourceLock(OperationContext* requestor,
                              const ResourceId& childResId,
                              const ResourceId& parentResId,
                              LockManager::Notifier* notifier = NULL)
            : ResourceLock(requestor, Locking::kExclusive, childResId, parentResId, notifier) { }

        ~ExclusiveResourceLock() { }
    };
} // namespace mongo
