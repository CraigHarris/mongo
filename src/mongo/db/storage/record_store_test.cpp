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

/**
 * RecordStore concurrency tests
 *
 * Testing concurrency requires multiple threads.  In this test, these are packaged as
 * instances of a ClientTransaction class.  The test's main thread creates instances
 * of ClientTransactions and communicates with them through a pair of producer/consumer
 * buffers.  The driver thread sends requests to ClientTransaction threads using a
 * TxCommandBuffer, and waits for responses in a TxResponseBuffer. This protocol
 * allows precise control over timing.
 */

#include <map>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include <boost/scoped_ptr.hpp>
#include <boost/thread/thread.hpp>
#include "mongo/bson/mutable/damage_vector.h"
#include "mongo/db/operation_context_impl.h"
#include "mongo/db/concurrency/lock_mgr.h"
#include "mongo/db/storage/record_store.h"
#include "mongo/db/storage/mmap_v1/mmap_v1_database_catalog_entry.h"
#include "mongo/platform/random.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/log.h"
#include "mongo/util/time_support.h"

using namespace std;

namespace mongo {

    // The RecordStore API takes and produces DiskLocs
    // But for mmap_v1, DiskLocs can change with concurrent execution
    // So our invariants are in terms of things that don't change with
    // concurrency
    class Context {
    public:
        size_t numRecords() const {
            boost::unique_lock<boost::mutex> lk(_mutex);
            return _insertedRecords.size() - _deletedRecords.size();
        }

        DiskLoc getLatestForDiskLoc(const DiskLoc& recId) const {
            boost::unique_lock<boost::mutex> lk(_mutex);
            map<DiskLoc,unsigned>::const_iterator it = _recIdMap.find(recId);
            verify(it != _recIdMap.end());
            verify(it->second < _insertedRecords.size());
            if (_deletedRecords.find(recId) != _deletedRecords.end())
                return DiskLoc();
            return _insertedRecords[it->second];
        }

        DiskLoc getByIndex(unsigned index) const {
            boost::unique_lock<boost::mutex> lk(_mutex);
            DiskLoc result =  _insertedRecords[index];
            if (_deletedRecords.find(result) == _deletedRecords.end())
                return result;
            else
                return DiskLoc();
        }

        void insertRecord(const DiskLoc& recId) {
            boost::unique_lock<boost::mutex> lk(_mutex);
            _recIdMap[recId] = _insertedRecords.size();
            _insertedRecords.push_back(recId);
        }

        void deleteRecord(const DiskLoc& recId) {
            boost::unique_lock<boost::mutex> lk(_mutex);
            _deletedRecords.insert(oldRecId);
        }

        void moveRecord(const DiskLoc& oldRecId, const DiskLoc& newRecId) {
            boost::unique_lock<boost::mutex> lk(_mutex);
            _recIdMap[newRecId] = _insertedRecords.size();
            _recIdMap[oldRecId] = _insertedRecords.size();
            _insertedRecords.push_back(newRecId);
            _deletedRecords.insert(oldRecId);
        }
        
    private:
        mutable boost::mutex _mutex;
        vector<DiskLoc> _insertedRecords;
        set<DiskLoc> _deletedRecords;
        map<DiskLoc,unsigned> _recIdMap;
    };

    enum TxCmd {
        kBegin,
        kCommit,
        kEnd,
        kNumRecords,
        kDataFor,
        kDelete,
        kInsert,
        kUpdateInPlace,
        kUpdateMove,
        kGetIterator,
        kIteratorEof,
        kIteratorCurr,
        kIteratorGetNext,
        kIteratorDataFor,
        kQuit
    };

    enum TxRsp {
        kSucceeded,
        kFailed,
        kBlocked,
        kAwakened,
        kAborted,
    };

    string toString(const mutablebson::DamageVector& damages) {
        stringstream ss;
        bool first=true;
        ss << "<";
        for (vector<mutablebson::DamageEvent>::const_iterator it = damages.begin(); it != damages.end(); ++it) {
            if (first) first=false;
            else ss << ", ";
            ss << "{"
               << "sourceOffset: " << it->sourceOffset
               << "targetOffset: " << it->targetOffset
               << "size: " << it->size
               << "}" << endl;
        }
        ss << ">";
        return ss.str();
    }

    class TxRequest {
    public:
        string toString() const {
            switch (cmd) {
            case kBegin:
                return str::stream() << "Begin\n";
            case kCommit:
                return str::stream() << "Commit\n";
            case kEnd:
                return str::stream() << "End\n";
            case kNumRecords:
                return str::stream() << "NumRecords" << "\n";
            case kDataFor:
                return str::stream() << "DataFor( " << loc.toString() << " )\n";
            case kDelete:
                return str::stream() << "Delete( " << loc.toString() << " )\n";
            case kInsert:
                return str::stream()
                    << "Insert"
                    << "( " << data
                    << ", " << len
                    << ", " << boolVal
                    << " )\n";
            case kUpdateInPlace:
                return str::stream()
                    << "UpdateInPlace( " << loc.toString()
                    << ", " << data 
                    << ", " << mongo::toString(damages)
                    << " )\n";
            case kUpdateMove:
                return str::stream()
                    << "UpdateMove( " << loc.toString()
                    << ", " << data
                    << ", " << len
                    << " )\n";
            case kGetIterator:
                return str::stream() << "GetIterator\n";
            case kIteratorEof:
                return str::stream() << "IteratorEof\n";
            case kIteratorCurr:
                return str::stream() << "IteratorCurr\n";
            case kIteratorGetNext:
                return str::stream() << "IteratorGetNext\n";
            case kIteratorDataFor:
                return str::stream() << "IteratorDataFor( " << loc.toString() << " )\n";
            }
        }
    public:
        TxRequest() { }
        TxRequest(const TxCmd& cmd) : cmd(cmd) { }
        TxRequest(const TxCmd& cmd, const char* data, int len) : cmd(cmd), data(strdup(data)), len(len) { }
        TxRequest(const TxCmd& cmd, const DiskLoc& recId) : cmd(cmd), loc(recId) { }
        TxRequest(const TxCmd& cmd,
                  const DiskLoc& recId,
                  bool boolVal,
                  const CollectionScanParams::Direction& dir)
            : cmd(cmd), loc(recId), boolVal(boolVal), dir(dir) { }
        TxRequest(const TxCmd& cmd, const DiskLoc& recId, const char* data, int len)
            : cmd(cmd), loc(recId), data(strdup(data)), len(len) { }
        TxRequest(const TxCmd& cmd, const DiskLoc& recId, const char* data, mutablebson::DamageVector damages)
            : cmd(cmd), loc(recId), data(strdup(data)), damages(damages) { }
        TxRequest(const TxCmd& cmd, RecordIterator* rit) : cmd(cmd), it(rit) { }
        TxRequest(const TxCmd& cmd, RecordIterator* rit, const DiskLoc& loc) : cmd(cmd), loc(loc), it(rit) { }
    public:
        TxCmd cmd;
        uint32_t reqNum;
        OperationContext* txn;
        DiskLoc loc;
        char* data;
        int len;
        bool boolVal;
        RecordIterator* it;
        mutablebson::DamageVector damages; // for update
        CollectionScanParams::Direction dir; // for getIterator
    };

    class TxResponse {
    public:
        string toString(const TxRsp& rspCode) const {
            switch(rspCode) {
            case kSucceeded: return "Success";
            case kFailed: return "Failure";
            case kBlocked: return "Blocked";
            case kAwakened: return "Awakened";
            case kAborted: return "Aborted";
            }
        }

        template <typename T>
        string toString(const char* label, T value) const {
            stringstream ss;
            ss << toString(rspCode);
            if (kSucceeded == rspCode)
                ss << label << value << endl;
            return ss.str();
        }

        string toString() const {
            switch (cmdCode) {
            case kBegin:
                return toString(rspCode);
            case kCommit:
                return toString(rspCode);
            case kEnd:
                return toString(rspCode);
            case kNumRecords:
                return toString("numRecords", num);
            case kDataFor:
                return toString("data", record.data());
            case kDelete:
                return toString(rspCode);
            case kInsert:
                return toString("recId", loc.toString());
            case kUpdateInPlace:
                return toString(rspCode);
            case kUpdateMove:
                return toString("recId", loc.toString());
            case kGetIterator:
                return toString(rspCode);
            case kIteratorEof:
                return toString("eof", boolVal);
            case kIteratorCurr:
                return toString("recId", loc.toString());
            case kIteratorGetNext:
                return toString("recId", loc.toString());
            case kIteratorDataFor:
                return toString("data", record.data());
            }
        }
    public:
        TxResponse() : status(Status::OK()) { }
        TxCmd cmdCode;
        TxRsp rspCode;
        AtomicWord<uint32_t> rspNum;
        DiskLoc loc;
        Status status;
        RecordData record;
        long long num;
        bool boolVal;
        RecordIterator* it;
    };

    class TxResponseBuffer {
    public:
        TxResponseBuffer() : _count(0), _readPos(0), _writePos(0) { }

        TxResponse* startPost(boost::unique_lock<boost::mutex>& guard) {
            while (_count == 10) _full.wait(guard);
            return &_buffer[_writePos];
        }

        void finishPost() {
            _buffer[_writePos++].rspNum.fetchAndAdd(1);
            _writePos %= 10;
            _count++;
            _empty.notify_one();
        }

        void post(const TxRsp& rspCode) {
            boost::unique_lock<boost::mutex> guard(_guard);
            TxResponse* rsp = startPost(guard);
            rsp->rspCode = rspCode;
            finishPost();
        }

        void post(const TxRsp& rspCode, long long num) {
            boost::unique_lock<boost::mutex> guard(_guard);
            TxResponse* rsp = startPost(guard);
            rsp->rspCode = rspCode;
            rsp->num = num;
            finishPost();
        }

        void post(const TxRsp& rspCode, RecordData rec ) {
            boost::unique_lock<boost::mutex> guard(_guard);
            TxResponse* rsp = startPost(guard);
            rsp->rspCode = rspCode;
            rsp->record = rec;
            finishPost();
        }

        void post(const TxRsp& rspCode, const Status& status) {
            boost::unique_lock<boost::mutex> guard(_guard);
            TxResponse* rsp = startPost(guard);
            rsp->rspCode = rspCode;
            rsp->status = status;
            finishPost();
        }

        void post(const TxRsp& rspCode, const Status& status, const DiskLoc& loc ) {
            boost::unique_lock<boost::mutex> guard(_guard);
            TxResponse* rsp = startPost(guard);
            rsp->rspCode = rspCode;
            rsp->status = status;
            rsp->loc = loc;
            finishPost();
        }

        void post(const TxRsp& rspCode, RecordIterator* it ) {
            boost::unique_lock<boost::mutex> guard(_guard);
            TxResponse* rsp = startPost(guard);
            rsp->rspCode = rspCode;
            rsp->it = it;
            finishPost();
        }

        void post(const TxRsp& rspCode, const DiskLoc& loc ) {
            boost::unique_lock<boost::mutex> guard(_guard);
            TxResponse* rsp = startPost(guard);
            rsp->rspCode = rspCode;
            rsp->loc = loc;
            finishPost();
        }

        TxResponse* consume() {
            boost::unique_lock<boost::mutex> guard(_guard);
            while (_count == 0)
                _empty.wait(guard);
            TxResponse* result = &_buffer[_readPos++];
            _readPos %= 10;
            _count--;
            _full.notify_one();
            return result;
        }

        boost::mutex _guard;
        boost::condition_variable _full;
        boost::condition_variable _empty;

        size_t _count;
        size_t _readPos;
        size_t _writePos;

        TxResponse _buffer[10];
    };

    class TxCommandBuffer {
    public:
        TxCommandBuffer() : _count(0), _readPos(0), _writePos(0) { }

        TxRequest* startPost(boost::unique_lock<boost::mutex>& guard) {
            while (_count == 10) _full.wait(guard);
            return &_buffer[_writePos];
        }

        void finishPost() {
            _writePos++;
            _writePos %= 10;
            _count++;
            _empty.notify_one();
        }

        void post(const TxRequest& request) {
            boost::unique_lock<boost::mutex> guard(_guard);
            TxRequest* req = startPost(guard);
            *req = request;
            finishPost();
        }
            
        void post(const TxCmd& cmd) {
            boost::unique_lock<boost::mutex> guard(_guard);
            TxRequest* req = startPost(guard);
            req->cmd = cmd;
            finishPost();
        }

        // used for begin, commit, end, numRecords
        void post(const TxCmd& cmd, OperationContext* txn) {
            boost::unique_lock<boost::mutex> guard(_guard);
            TxRequest* req = startPost(guard);
            req->cmd = cmd;
            req->txn = txn;
            finishPost();
        }

        // used for dataFor, deleteRecord
        void post(const TxCmd& cmd, OperationContext* txn, const DiskLoc& loc) {
            boost::unique_lock<boost::mutex> guard(_guard);
            TxRequest* req = startPost(guard);
            req->cmd = cmd;
            req->txn = txn;
            req->loc = loc;
            finishPost();
        }

        // used for insertRecord
        void post(const TxCmd& cmd, OperationContext* txn, char* data, int len, bool quota) {
            boost::unique_lock<boost::mutex> guard(_guard);
            TxRequest* req = startPost(guard);
            req->cmd = cmd;
            req->txn = txn;
            req->data = data;
            req->len = len;
            req->boolVal = quota;
            finishPost();
        }

        // used for updateMove
        void post(const TxCmd& cmd, OperationContext* txn, const DiskLoc& loc,
                  char* data, int len, bool quota) {
            boost::unique_lock<boost::mutex> guard(_guard);
            TxRequest* req = startPost(guard);
            req->cmd = cmd;
            req->txn = txn;
            req->loc = loc;
            req->data = data;
            req->len = len;
            req->boolVal = quota;
            finishPost();
        }

        // used for updateInPlace
        void post(const TxCmd& cmd, OperationContext* txn, const DiskLoc& loc,
                  char* data, const mutablebson::DamageVector& damages) {
            boost::unique_lock<boost::mutex> guard(_guard);
            TxRequest* req = startPost(guard);
            req->cmd = cmd;
            req->txn = txn;
            req->loc = loc;
            req->data = data;
            req->damages = damages;
            finishPost();
        }

        // used for getIterator
        void post(const TxCmd& cmd, OperationContext* txn, const DiskLoc& loc,
                  bool isTailable, const CollectionScanParams::Direction& dir) {
            boost::unique_lock<boost::mutex> guard(_guard);
            TxRequest* req = startPost(guard);
            req->cmd = cmd;
            req->txn = txn;
            req->loc = loc;
            req->boolVal = isTailable;
            req->dir = dir;
            finishPost();
        }

        // used for getIterator{Eof,Curr,GetNext}
        void post(const TxCmd& cmd, OperationContext* txn, RecordIterator* it) {
            boost::unique_lock<boost::mutex> guard(_guard);
            TxRequest* req = startPost(guard);
            req->cmd = cmd;
            req->txn = txn;
            req->it = it;
            finishPost();
        }

        // used for getIteratorDataFor
        void post(const TxCmd& cmd, OperationContext* txn, RecordIterator* it, const DiskLoc& loc) {
            boost::unique_lock<boost::mutex> guard(_guard);
            TxRequest* req = startPost(guard);
            req->cmd = cmd;
            req->txn = txn;
            req->loc = loc;
            req->it = it;
            finishPost();
        }

        TxRequest* consume() {
            boost::unique_lock<boost::mutex> guard(_guard);
            while (_count == 0)
                _empty.wait(guard);
            TxRequest* result = &_buffer[_readPos++];
            _readPos %= 10;
            _count--;
            _full.notify_one();
            return result;
        }

        boost::mutex _guard;
        boost::condition_variable _full;
        boost::condition_variable _empty;

        size_t _count;
        size_t _readPos;
        size_t _writePos;

        TxRequest _buffer[10];
    };

    class ClientTransaction : public LockManager::Notifier {
    public:
        // these are called in the main driver program
    
        ClientTransaction(unsigned id, Context* ctx, RecordStore* rs)
            : _workerNum(id)
            , _ctx(ctx)
            , _rs(rs)
            , _thr(&ClientTransaction::processCmd, this) { }

        virtual ~ClientTransaction() { _thr.join(); }

        void invoke(const TxRequest& req) {
            _cmd.post(req);
        }

        void begin(OperationContext* txn, const TxRsp& rspCode) {
            _cmd.post(kBegin, txn);
            TxResponse* rsp = _rsp.consume();
            ASSERT(rspCode == rsp->rspCode);
        }

        void commit(OperationContext* txn, const TxRsp& rspCode) {
            _cmd.post(kCommit, txn);
            TxResponse* rsp = _rsp.consume();
            ASSERT(rspCode == rsp->rspCode);
        }

        void end(OperationContext* txn, const TxRsp& rspCode) {
            _cmd.post(kEnd, txn);
            TxResponse* rsp = _rsp.consume();
            ASSERT(rspCode == rsp->rspCode);
        }

        void numRecords(OperationContext* txn, const TxRsp& rspCode) {
            _cmd.post(kNumRecords, txn);
            TxResponse* rsp = _rsp.consume();
            ASSERT(rspCode == rsp->rspCode);
        }

        void dataFor(OperationContext* txn, const TxRsp& rspCode, const DiskLoc& loc) {
            _cmd.post(kDataFor, txn, loc);
            TxResponse* rsp = _rsp.consume();
            ASSERT(rspCode == rsp->rspCode);
        }

        void deleteRecord(OperationContext* txn, const TxRsp& rspCode, const DiskLoc& loc) {
            _cmd.post(kDelete, txn, loc);
            TxResponse* rsp = _rsp.consume();
            ASSERT(rspCode == rsp->rspCode);
        }

        void insertRecord(OperationContext* txn, const TxRsp& rspCode, char* data, int len, bool quota) {
            _cmd.post(kInsert, txn, data, len, quota);
            TxResponse* rsp = _rsp.consume();
            ASSERT(rspCode == rsp->rspCode);
        }

        void updateMove(OperationContext* txn, const TxRsp& rspCode, const DiskLoc& loc, char* data,
                        int len, bool quota) {
            _cmd.post(kUpdateMove, txn, loc, data, len, quota);
            TxResponse* rsp = _rsp.consume();
            ASSERT(rspCode == rsp->rspCode);
        }

        void updateInPlace(OperationContext* txn, const TxRsp& rspCode, const DiskLoc& loc, char* data,
                           const mutablebson::DamageVector& damages) {
            _cmd.post(kUpdateInPlace, txn, loc, data, damages);
            TxResponse* rsp = _rsp.consume();
            ASSERT(rspCode == rsp->rspCode);
        }

        void getIterator(OperationContext* txn, const TxRsp& rspCode, const DiskLoc& loc, bool isTailable,
                         const CollectionScanParams::Direction& dir) {
            _cmd.post(kGetIterator, txn, loc, isTailable, dir);
            TxResponse* rsp = _rsp.consume();
            ASSERT(rspCode == rsp->rspCode);
        }

        void iteratorEof(OperationContext* txn, const TxRsp& rspCode, RecordIterator* it) {
            _cmd.post(kIteratorEof, txn, it);
            TxResponse* rsp = _rsp.consume();
            ASSERT(rspCode == rsp->rspCode);
        }

        void iteratorCurr(OperationContext* txn, const TxRsp& rspCode, RecordIterator* it) {
            _cmd.post(kIteratorCurr, txn, it);
            TxResponse* rsp = _rsp.consume();
            ASSERT(rspCode == rsp->rspCode);
        }

        void iteratorGetNext(OperationContext* txn, const TxRsp& rspCode, RecordIterator* it) {
            _cmd.post(kIteratorGetNext, txn, it);
            TxResponse* rsp = _rsp.consume();
            ASSERT(rspCode == rsp->rspCode);
        }

        void iteratorDataFor(OperationContext* txn, const TxRsp& rspCode, RecordIterator* it,
                                const DiskLoc& loc) {
            _cmd.post(kIteratorDataFor, txn, it, loc);
            TxResponse* rsp = _rsp.consume();
            ASSERT(rspCode == rsp->rspCode);
        }

        void quit() {
            _cmd.post(kQuit);
        }

    
    
        // these are run within the client threads
        void processCmd() {
            bool more = true;
            while (more) {
                try {
                    TxRequest* req = _cmd.consume();
                    switch (req->cmd) {
                    case kBegin:
                        req->txn->getTransaction()->enterScope();
                        req->txn->recoveryUnit()->beginUnitOfWork();
                        _rsp.post(kSucceeded);
                        break;
                    case kCommit:
                        req->txn->recoveryUnit()->commitUnitOfWork();
                        req->txn->getTransaction()->exitScope();
                        req->txn->getTransaction()->enterScope();
                        _rsp.post(kSucceeded);
                        break;
                    case kEnd:
                        req->txn->recoveryUnit()->commitUnitOfWork();
                        req->txn->getTransaction()->exitScope();
                        _rsp.post(kSucceeded);
                        break;
                    case kNumRecords:
                        _rsp.post(kSucceeded, _rs->numRecords(req->txn));
                        break;
                    case kDataFor:
                        DiskLoc latestLoc = _ctx->getLatestForDiskLoc(req->loc);
                        if (latestLoc.isNull())
                            _rsp.post(kFailed);
                        else
                            _rsp.post(kSucceeded, _rs->dataFor(req->txn, latestLoc));
                        break;
                    case kDelete:
                        _rs->deleteRecord(req->txn, req->loc);
                        _ctx->deleteRecord(req->loc);
                        _rsp.post(kSucceeded);
                        break;
                    case kInsert:
                    {
                        StatusWith<DiskLoc> res = _rs->insertRecord(req->txn, req->data, req->len, req->boolVal);
                        if (Status::OK() == res.getStatus())
                            _ctx->insertRecord(res.getValue());
                        _rsp.post(kSucceeded, res.getStatus(), res.getValue());
                    }
                    break;
                    case kUpdateMove:
                    {
                        DiskLoc latestLoc = _ctx->getLatestForDiskLoc(req->loc);
                        if (latestLoc.isNull())
                            _rsp.post(kFailed);
                        StatusWith<DiskLoc> res = _rs->updateRecord(req->txn, latestLoc, req->data, req->len,
                                                                    req->boolVal, NULL);
                        if (Status::OK() == res.getStatus())
                            _ctx->moveRecord(req->loc, res.getValue());
                        _rsp.post(kSucceeded, res.getStatus(), res.getValue());
                    }
                    break;
                    case kUpdateInPlace:
                        DiskLoc latestLoc = _ctx->getLatestForDiskLoc(req->loc);
                        if (latestLoc.isNull())
                            _rsp.post(kFailed);
                        _rsp.post(kSucceeded, _rs->updateWithDamages(req->txn, latestLoc, req->data, req->damages));
                        break;
                    case kGetIterator:
                        _rit = _rs->getIterator(req->txn, req->loc, req->boolVal, req->dir);
                        _rsp.post(kSucceeded);
                        break;
                    case kIteratorEof:
                        _rsp.post(kSucceeded, _rit->isEOF());
                        break;
                    case kIteratorCurr:
                        _iterLoc = _rit->curr();
                        _rsp.post(kSucceeded, _iterLoc);
                        break;
                    case kIteratorGetNext:
                        _iterLoc = _rit->getNext();
                        _rsp.post(kSucceeded, _iterLoc);
                        break;
                    case kIteratorDataFor:
                        _rsp.post(kSucceeded, _rit->dataFor(_iterLoc));
                        break;
                    case kQuit:
                    default:
                        more = false;
                        break;
                    }
                    _rsp.post(kSucceeded);
                } catch (const LockManager::AbortException& err) {
                    _rsp.post(kAborted);
                    return;
                }
            }
        }

        // inherited from Notifier, used by LockManager::acquire
        virtual void operator()(const Transaction* blocker) {
            _rsp.post(kBlocked);
        }

    private:
        unsigned _workerNum;
        Context* _ctx;
        RecordStore* _rs;
        TxCommandBuffer _cmd;
        TxResponseBuffer _rsp;
        RecordIterator* _rit;
        DiskLoc _iterLoc;
        boost::thread _thr;
        vector<char> _valuesForWorkersField; // indexed by docId

        // map docId to set of update changes that caused doc to move
        // the set has offsets from start of doc to location of added char
        // always one past the previous size of the document

        map<unsigned, set<unsigned> > _growth;
    };

/*

  WriteCommand:= kDelete DiskLoc
  | kUpdateMove DiskLoc ...
  | kUpdateInPlace DiskLoc ...
  | kInsert ...;

  ReadCommand:= kDataFor
  | kNumRecords
  | kDataSize
  | Iterator

  Iterator:= kGetIterator IteratorCommand

  IteratorCommand:= kIteratorEof 
  | kIteratorEof kIteratorCurr kIteratorGetNext
  | kIteratorEof kIteratorCurr kIteratorDataFor kIteratorGetNext

  Command:= WriteCommand | ReadCommand | Iterator
  CommandList:= Command
  | Command CommandList

  TransactionEnd:= kEnd | kCommit kEnd

  SimpleTransaction:= kBegin CommandList TransactionEnd

  SimpleTransactionList:= SimpleTransaction
  | SimpleTransaction SimpleTransactionList

  NestedTransaction:= kBegin SimpleTransactionList TransactionEnd
  | kBegin SimpleTransactionList CommandList TransactionEnd
  | kBegin CommandList SimpleTransactionList TransactionEnd
  | kBegin CommandList SimpleTransactionList CommandList TransactionEnd

  NestedTransactionList:= NestedTransaction
  | NestedTransaction NestedTransactionList

  Transaction:= 
  Operation:= ReadCommand | Transaction

*/

/*
// generator: produce set of sequences of operations

hand crafted input:
before first insert, numRecords==0;
after insert
numRecords incremented
dataFor matches inserted record
iterator yields inserted record
after delete
numRecords decremented
iterator doesn't find record
after update
numRecords unchanged
iterator finds new record and not old
no changes if commit not called

// random_shuffle: takes set of sequences of operations, produces randomized sequence
// power_shuffle: takes set of sequences of operations, produces power-set of all sequences
// executor: takes sequence, executes
// driver: 
//    take set<sequence<op>>, 
//    execute each sequence sequentially on one thread, recording results
//    if cardinality low enough, call call power_shuffle and compare results to sequential results
//    else call random_shuffle and compare results to sequential results
*/

/*
  insert: generate random #fields < 10 and values < 10;
  update: limit to 24 workers

  if new size of record > original dataSize, record will move
*/        

    struct Operation {

        Operation() { }
        Operation(TxRequest req) : txn(0), req(req) { }
        Operation(const Operation& other) {
            txn = other.txn;
            req = other.req;
        }
        static Operation begin() {
            return Operation(TxRequest(kBegin));
        }
        static Operation commit() {
            return Operation(TxRequest(kCommit));
        }
        static Operation end() {
            return Operation(TxRequest(kEnd));
        }
        static Operation insertRecord(const char* newData, int newLen) {
            return Operation(TxRequest(kInsert, newData, newLen));
        }
        static Operation updateMove(const DiskLoc& recId) {
            return Operation(TxRequest(kUpdateMove, recId));
        }
        static Operation updateInPlace(const DiskLoc& recId) {
            return Operation(TxRequest(kUpdateInPlace, recId));
        }
        static Operation deleteRecord(const DiskLoc& recId) {
            return Operation(TxRequest(kDelete, recId));
        }
        static Operation getIterator(int dir=1) {
            return Operation(TxRequest(kGetIterator, dir));
        }
        static Operation iteratorDataFor() {
            return Operation(TxRequest(kIteratorDataFor));
        }
        static Operation iteratorCurr() {
            return Operation(TxRequest(kIteratorCurr));
        }
        static Operation iteratorGetNext() {
            return Operation(TxRequest(kIteratorGetNext));
        }
        static Operation iteratorEof() {
            return Operation(TxRequest(kIteratorEof));
        }

        unsigned txn;
        TxRequest req;
        TxResponse rsp;
    };

    typedef vector<Operation> Schedule;
    typedef vector<Schedule> Workload;

    class WorkLoadDescription {
    public:
        WorkLoadDescription(unsigned numInitialDocs = 10,
                            unsigned numBulkLoaders = 0,
                            unsigned numForwardCollectionScanners = 0,
                            unsigned numBackwardCollectionScanners = 0,
                            unsigned numPointInPlaceUpdaters = 0,
                            unsigned numPointMoveUpdaters = 0,
                            unsigned numPointReaders = 0)
            : _numInitialDocs(numInitialDocs)
            , _numBulkLoaders(numBulkLoaders)
            , _numForwardCollectionScanners(numForwardCollectionScanners)
            , _numBackwardCollectionScanners(numBackwardCollectionScanners)
            , _numPointInPlaceUpdaters(numPointInPlaceUpdaters)
            , _numPointMoveUpdaters(numPointMoveUpdaters)
            , _numPointReaders(numPointReaders)
            { }

        void setWorkload(const Workload& workload) { _workload = workload; }
        Workload getWorkload() const { return _workload; }
        
        unsigned getNumWorkers() const { return _workload.size(); }

        Workload generate() {
            PseudoRandom rnd(time(0));

            for (unsigned ix=0; ix < _numBulkLoaders; ix++) {
                Schedule nextSchedule;
                nextSchedule.push_back(Operation::begin());
                string doc;
                for (int jx=0; jx < _maxWorkers; jx++) {
                    doc.push_back('a');
                }
                for (int jx=0; jx < _numInitialDocs; jx++) {
                    nextSchedule.push_back(Operation::insertRecord(doc.c_str(), _maxWorkers));
                }
                nextSchedule.push_back(Operation::commit());
                nextSchedule.push_back(Operation::end());
                _workload.push_back(nextSchedule);
            }

            for (unsigned ix=0; ix < _numForwardCollectionScanners; ix++) {
                Schedule nextSchedule;
                nextSchedule.push_back(Operation::getIterator());
                for (unsigned jx=0; jx < _numInitialDocs; jx++) {
                    nextSchedule.push_back(Operation::iteratorEof());
                    nextSchedule.push_back(Operation::iteratorGetNext());
                    nextSchedule.push_back(Operation::iteratorDataFor());
                }
                _workload.push_back(nextSchedule);
            }

            for (unsigned ix=0; ix < _numBackwardCollectionScanners; ix++) {
                Schedule nextSchedule;
                nextSchedule.push_back(Operation::getIterator(-1));
                for (unsigned jx=0; jx < _numInitialDocs; jx++) {
                    nextSchedule.push_back(Operation::iteratorEof());
                    nextSchedule.push_back(Operation::iteratorGetNext());
                    nextSchedule.push_back(Operation::iteratorDataFor());
                }
                _workload.push_back(nextSchedule);
            }

            for (unsigned ix=0; ix < _numPointInPlaceUpdaters; ix++) {
                Schedule nextSchedule;
                nextSchedule.push_back(Operation::getIterator());
                for (unsigned jx=0; jx < _numInitialDocs; jx++) {
                    nextSchedule.push_back(Operation::iteratorEof());
                    nextSchedule.push_back(Operation::iteratorGetNext());
                    nextSchedule.push_back(Operation::iteratorDataFor());
                    nextSchedule.push_back(Operation::begin());
                    nextSchedule.push_back(Operation::updateInPlace(docId));
                    nextSchedule.push_back(Operation::commit());
                    nextSchedule.push_back(Operation::end());
                }
                _workload.push_back(nextSchedule);
            }

            for (unsigned ix=0; ix < _numPointMoveUpdaters; ix++) {
                Schedule nextSchedule;
                nextSchedule.push_back(Operation::getIterator());
                for (unsigned jx=0; jx < _numInitialDocs; jx++) {
                    nextSchedule.push_back(Operation::iteratorEof());
                    nextSchedule.push_back(Operation::iteratorGetNext());
                    nextSchedule.push_back(Operation::iteratorDataFor());
                    nextSchedule.push_back(Operation::begin());
                    nextSchedule.push_back(Operation::updateMove(docId));
                    nextSchedule.push_back(Operation::commit());
                    nextSchedule.push_back(Operation::end());
                }
                _workload.push_back(nextSchedule);
            }

            for (unsigned ix=0; ix < _numPointInPlaceUpdaters; ix++) {
                unsigned docId = rnd.getNextInt64(?numRecords?);
                Schedule nextSchedule;
                nextSchedule.push_back(Operation::begin());
                nextSchedule.push_back(Operation::numRecords());
                nextSchedule.push_back(Operation::dataFor(docId));
                nextSchedule.push_back(Operation::updateInPlace(docId);
                nextSchedule.push_back(Operation::commit());
                nextSchedule.push_back(Operation::end());
                _workload.push_back(nextSchedule);
            }

            for (unsigned ix=0; ix < _numPointMoveUpdaters; ix++) {
                unsigned docId = rnd.getNextInt64(?numRecords?);
                Schedule nextSchedule;
                nextSchedule.push_back(Operation::begin());
                nextSchedule.push_back(Operation::numRecords());
                nextSchedule.push_back(Operation::dataFor(docId));
                nextSchedule.push_back(Operation::updateMove(docId);
                nextSchedule.push_back(Operation::commit());
                nextSchedule.push_back(Operation::end());
                _workload.push_back(nextSchedule);
            }

            return _workload;
        }
    
    private:
        unsigned _numInitialDocs;
        unsigned _numBulkLoaders;
        unsigned _numForwardCollectionScanners;
        unsigned _numBackwardCollectionScanners;
        unsigned _numPointInPlaceUpdaters;
        unsigned _numPointMoveUpdaters;
        unsigned _numPointReaders;
        Workload _workload;
    };

    class WorkLoadScheduler {
    public:
        WorkLoadScheduler(const Workload& workload) : _workload(workload) { }

        Schedule scheduleRandomly() {
            Schedule result;
            vector<ScheduleState> state;
            for (unsigned ix = 0; ix < _workload.size(); ++ix) {
                state.push_back(ScheduleState(_workload[ix].begin(), _workload[ix].end()));
            }
            PseudoRandom rnd(time(0));
            while (!state.empty()) {
                int64_t nextScheduleIndex = rnd.nextInt64(state.size());
                ScheduleState ss = state[nextScheduleIndex];
                if (ss) {
                    result.push_back(*ss++);
                }
                else {
                    state.erase(state.at(nextScheduleIndex));
                }
            }
        
            return result;
        }

        set<Schedule> scheduleExhaustively() {
            set<Schedule> result;
            vector<ScheduleState> state;
            for (unsigned ix = 0; ix < _workload.size(); ++ix) {
                state.push_back(ScheduleState(_workload[ix].begin(), _workload[ix].end()));
            }
        
            return result;
        }
    private:
        class ScheduleState {
        public:
            ScheduleState(Schedule::const_iterator next, const Schedule::const_iterator& last)
                : _next(next), _last(last) { }
            operator bool() const {return _next != _last;}
            Operation operator*() const {return *_next;}
            ScheduleState& operator++() {
                _next++;
                return *this;
            }
            ScheduleState operator++(int) {
                ScheduleState result(*this);
                ++(*this);
                return result;
            }
            
        private:
            Schedule::const_iterator& _next;
            const Schedule::const_iterator& _last;
        };

        const Workload& _workload;
    };

    void execute(const WorkLoadDescription& wld, const vector<Operation>& schedule) {
        Context ctx;

        // launch worker threads
        vector<ClientTransaction*> workers;
        for(unsigned ix=0; ix < wld.getNumWorkers(); ++ix) {
            workers.push_back(new ClientTransaction(ix, &ctx, wld.getRecordStore()));
        }

        // post operation requests against target workers, without waiting for responses
        for(Schedule::iterator it = schedule.begin(); it != schedule.end(); ++it) {
            workers[it->txn]->invoke(it->req);
        }

        // shutdown worker threads
        for (unsigned ix=0; ix < wld.getNumWorkers(); ++ix) {
            ClientTransaction* nextWorker = workers[ix];
            if (nextWorker->isAlive()) {
                nextWorker->quit();
            }
            delete nextWorker;
        }
    }

    TEST(RecordStoreTest, RandomSchedule) {
        OperationContextImpl txn;
        Context ctx;
        MMAPV1DatabaseCatalogEntry db( &txn, "foo", "?", false?, false? );

        db.createCollection( &txn, "foo.bar", CollectionOptions(), true );
        RecordStore* rs = db.getRecordStore( &txn, "foo.bar" );

        WorkLoadDescription wld;

        // load initial records
        for (int ix=0; ix < wld.getNumInitialDocs(); ++ix) {
            string doc;
            for (unsigned jx=0; jx < wld.getMaxNumWorkers(); ++jx) {
                doc.push_back('a');
            }
            StatusWith<DiskLoc> recId = rs->insertRecord(&txn, doc.c_str(), doc.len(), false);
            ASSERT_OK(recId.getStatus());
            context.add(recId.getValue(), rs->dataFor(&txn, recId.getValue()));
        }

        Schedule s1;
        s1.push_back(Operation::begin());
        s1.push_back(Operation::updateMove(recId, newData, newLen));
        s1.push_back(Operation::updateMove(recId, newData, newLen));
        s1.push_back(Operation::commit());
        s1.push_back(Operation::end());

        Schedule s2;
        s2.push_back(Operation::getIterator(recId, isTailable, dir));
        s2.push_back(Operation::iteratorGetNext());
        s2.push_back(Operation::iteratorGetNext());
        s2.push_back(Operation::iteratorGetNext());
        s2.push_back(Operation::iteratorEof());

        Schedule s3;
        s3.push_back(Operation::begin());
        s3.push_back(Operation::deleteRecord(recId));
        s3.push_back(Operation::commit());
        s3.push_back(Operation::end());

        Workload workload;
        workload.push_back(s1);
        workload.push_back(s2);
        workload.push_back(s3);

        WorkLoadScheduler wls(workload);
        for (int ix=0; ix < NUM_RANDOM_TRIES; ++ix) {
            execute(wld, wls.scheduleRandomly());
        }
    }

} // namespace mongo
