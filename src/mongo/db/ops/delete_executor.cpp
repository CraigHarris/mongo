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

#include "mongo/platform/basic.h"

#include "mongo/db/ops/delete_executor.h"

#include "mongo/db/catalog/collection.h"
#include "mongo/db/catalog/database.h"
#include "mongo/db/client.h"
#include "mongo/db/curop.h"
#include "mongo/db/ops/delete_request.h"
#include "mongo/db/query/canonical_query.h"
#include "mongo/db/query/get_runner.h"
#include "mongo/db/query/lite_parsed_query.h"
#include "mongo/db/query/query_planner_common.h"
#include "mongo/db/repl/is_master.h"
#include "mongo/db/repl/oplog.h"
#include "mongo/db/structure/catalog/namespace_details.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/mongoutils/str.h"

namespace mongo {

    DeleteExecutor::DeleteExecutor(const DeleteRequest* request) :
        _request(request),
        _canonicalQuery(),
        _isQueryParsed(false) {
    }

    DeleteExecutor::~DeleteExecutor() {}

    Status DeleteExecutor::prepare() {
        if (_isQueryParsed)
            return Status::OK();

        dassert(!_canonicalQuery.get());

        if (CanonicalQuery::isSimpleIdQuery(_request->getQuery())) {
            _isQueryParsed = true;
            return Status::OK();
        }

        CanonicalQuery* cqRaw;
        Status status = CanonicalQuery::canonicalize(_request->getNamespaceString().ns(),
                                                     _request->getQuery(),
                                                     &cqRaw);
        if (status.isOK()) {
            _canonicalQuery.reset(cqRaw);
            _isQueryParsed = true;
        }
        else if (status == ErrorCodes::NoClientContext) {
            // _isQueryParsed is still false, but execute() will try again under the lock.
            status = Status::OK();
        }
        return status;
    }

    long long DeleteExecutor::execute() {
        const bool canYield = !_request->isGod() && (
                _canonicalQuery.get() ?
                !QueryPlannerCommon::hasNode(_canonicalQuery->root(), MatchExpression::ATOMIC) :
                LiteParsedQuery::isQueryIsolated(_request->getQuery()));

        Runner* rawRunner;
        if (_canonicalQuery.get()) {
            uassertStatusOK(getRunner(collection, _canonicalQuery.release(), &rawRunner));
        }
        else {
            CanonicalQuery* ignored;
            uassertStatusOK(getRunner(collection,
                                      ns.ns(),
                                      _request->getQuery(),
                                      &rawRunner,
                                      &ignored));
        }

        auto_ptr<Runner> runner(rawRunner);
        auto_ptr<ScopedRunnerRegistration> safety;

        if (canYield) {
            safety.reset(new ScopedRunnerRegistration(runner.get()));
            runner->setYieldPolicy(Runner::YIELD_AUTO);
        }

        DiskLoc rloc;
        Runner::RunnerState state;
        CurOp* curOp = cc().curop();
        int oldYieldCount = curOp->numYields();
        while (Runner::RUNNER_ADVANCED == (state = runner->getNext(NULL, &rloc))) {
            if (oldYieldCount != curOp->numYields()) {
                uassert(ErrorCodes::NotMaster,
                        str::stream() << "No longer primary while removing from " << ns.ns(),
                        !logop || isMasterNs(ns.ns().c_str()));
                oldYieldCount = curOp->numYields();
            }
            BSONObj toDelete;

            // TODO: do we want to buffer docs and delete them in a group rather than
            // saving/restoring state repeatedly?
            runner->saveState();
            collection->deleteDocument(rloc, false, false, logop ? &toDelete : NULL );
            runner->restoreState();

            nDeleted++;

            if (logop) {
                if ( toDelete.isEmpty() ) {
                    problem() << "deleted object without id, not logging" << endl;
                }
                else {
                    bool replJustOne = true;
                    logOp("d", ns.ns().c_str(), toDelete, 0, &replJustOne);
                }
            }

            if (!_request->isMulti()) {
                break;
            }

            if (!_request->isGod()) {
                getDur().commitIfNeeded();
            }

            if (debug && _request->isGod() && nDeleted == 100) {
                log() << "warning high number of deletes with god=true "
                      << " which could use significant memory b/c we don't commit journal";
            }
        }

        return nDeleted;
    }

}  // namespace mongo
