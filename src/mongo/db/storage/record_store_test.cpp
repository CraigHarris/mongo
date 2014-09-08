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

#include <boost/scoped_ptr.hpp>
#include <boost/thread/thread.hpp>
#include "mongo/unittest/unittest.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/concurrency/lock_mgr.h"
#include "mongo/db/storage/record_store.h"
#include "mongo/util/log.h"
#include "mongo/util/time_support.h"

namespace mongo {

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

    class TxRequest {
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
        TxResponse() : status(Status::OK()) { }
        TxRsp rspCode;
        AtomicWord<uint32_t> rspNum;
        DiskLoc loc;
        Status status;
        RecordData record;
        long long num;
        RecordIterator* it;
    };

    class TxResponseBuffer {
    public:
        TxResponseBuffer() : _count(0), _readPos(0), _writePos(0) { }

        void post(const TxRsp& rspCode) {
            boost::unique_lock<boost::mutex> guard(_guard);
            TxResponse* buffer = startPost(guard);
            buffer->rspCode = rspCode;
            finishPost();
        }

        void post(const TxRsp& rspCode, long long num) {
            boost::unique_lock<boost::mutex> guard(_guard);
            TxResponse* buffer = startPost(guard);
            buffer->rspCode = rspCode;
            buffer->num = num;
            finishPost();
        }

        void post(const TxRsp& rspCode, RecordData rec ) {
            boost::unique_lock<boost::mutex> guard(_guard);
            TxResponse* buffer = startPost(guard);
            buffer->rspCode = rspCode;
            buffer->record = rec;
            finishPost();
        }

        void post(const TxRsp& rspCode, const Status& status) {
            boost::unique_lock<boost::mutex> guard(_guard);
            TxResponse* buffer = startPost(guard);
            buffer->rspCode = rspCode;
            buffer->status = status;
            finishPost();
        }

        void post(const TxRsp& rspCode, const Status& status, const DiskLoc& loc ) {
            boost::unique_lock<boost::mutex> guard(_guard);
            TxResponse* buffer = startPost(guard);
            buffer->rspCode = rspCode;
            buffer->status = status;
            buffer->loc = loc;
            finishPost();
        }

        void post(const TxRsp& rspCode, RecordIterator* it ) {
            boost::unique_lock<boost::mutex> guard(_guard);
            TxResponse* buffer = startPost(guard);
            buffer->rspCode = rspCode;
            buffer->it = it;
            finishPost();
        }

        void post(const TxRsp& rspCode, const DiskLoc& loc ) {
            boost::unique_lock<boost::mutex> guard(_guard);
            TxResponse* buffer = startPost(guard);
            buffer->rspCode = rspCode;
            buffer->loc = loc;
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
            
        void post(const TxCmd& cmd) {
            boost::unique_lock<boost::mutex> guard(_guard);
            TxRequest* buffer = startPost(guard);
            buffer[_writePos].cmd = cmd;
            finishPost();
        }

        // used for begin, commit, end, numRecords
        void post(const TxCmd& cmd, OperationContext* txn) {
            boost::unique_lock<boost::mutex> guard(_guard);
            TxRequest* buffer = startPost(guard);
            buffer[_writePos].cmd = cmd;
            buffer[_writePos].txn = txn;
            finishPost();
        }

        // used for dataFor, deleteRecord
        void post(const TxCmd& cmd, OperationContext* txn, const DiskLoc& loc) {
            boost::unique_lock<boost::mutex> guard(_guard);
            TxRequest* buffer = startPost(guard);
            buffer[_writePos].cmd = cmd;
            buffer[_writePos].txn = txn;
            buffer[_writePos].loc = loc;
            finishPost();
        }

        // used for insertRecord
        void post(const TxCmd& cmd, OperationContext* txn, char* data, int len, bool quota) {
            boost::unique_lock<boost::mutex> guard(_guard);
            TxRequest* buffer = startPost(guard);
            buffer[_writePos].cmd = cmd;
            buffer[_writePos].txn = txn;
            buffer[_writePos].data = data;
            buffer[_writePos].len = len;
            buffer[_writePos].boolVal = quota;
            finishPost();
        }

        // used for updateMove
        void post(const TxCmd& cmd, OperationContext* txn, const DiskLoc& loc,
                  char* data, int len, bool quota) {
            boost::unique_lock<boost::mutex> guard(_guard);
            TxRequest* buffer = startPost(guard);
            buffer[_writePos].cmd = cmd;
            buffer[_writePos].txn = txn;
            buffer[_writePos].loc = loc;
            buffer[_writePos].data = data;
            buffer[_writePos].len = len;
            buffer[_writePos].boolVal = quota;
            finishPost();
        }

        // used for updateInPlace
        void post(const TxCmd& cmd, OperationContext* txn, const DiskLoc& loc,
                  char* data, const mutablebson::DamageVector& damages) {
            boost::unique_lock<boost::mutex> guard(_guard);
            TxRequest* buffer = startPost(guard);
            buffer[_writePos].cmd = cmd;
            buffer[_writePos].txn = txn;
            buffer[_writePos].loc = loc;
            buffer[_writePos].data = data;
            buffer[_writePos].damages = damages;
            finishPost();
        }

        // used for getIterator
        void post(const TxCmd& cmd, OperationContext* txn, const DiskLoc& loc,
                  bool isTailable, const CollectionScanParams::Direction& dir) {
            boost::unique_lock<boost::mutex> guard(_guard);
            TxRequest* buffer = startPost(guard);
            buffer[_writePos].cmd = cmd;
            buffer[_writePos].txn = txn;
            buffer[_writePos].loc = loc;
            buffer[_writePos].boolVal = isTailable;
            buffer[_writePos].dir = dir;
            finishPost();
        }

        // used for getIterator{Eof,Curr,GetNext}
        void post(const TxCmd& cmd, OperationContext* txn, RecordIterator* it) {
            boost::unique_lock<boost::mutex> guard(_guard);
            TxRequest* buffer = startPost(guard);
            buffer[_writePos].cmd = cmd;
            buffer[_writePos].txn = txn;
            buffer[_writePos].it = it;
            finishPost();
        }

        // used for getIteratorDataFor
        void post(const TxCmd& cmd, OperationContext* txn, RecordIterator* it, const DiskLoc& loc) {
            boost::unique_lock<boost::mutex> guard(_guard);
            TxRequest* buffer = startPost(guard);
            buffer[_writePos].cmd = cmd;
            buffer[_writePos].txn = txn;
            buffer[_writePos].loc = loc;
            buffer[_writePos].it = it;
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
    
    ClientTransaction(RecordStore* rs)
        : _rs(rs)
        , _thr(&ClientTransaction::processCmd, this) { }

    virtual ~ClientTransaction() { _thr.join(); }

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

    void getIteratorEof(OperationContext* txn, const TxRsp& rspCode, RecordIterator* it) {
        _cmd.post(kIteratorEof, txn, it);
        TxResponse* rsp = _rsp.consume();
        ASSERT(rspCode == rsp->rspCode);
    }

    void getIteratorCurr(OperationContext* txn, const TxRsp& rspCode, RecordIterator* it) {
        _cmd.post(kIteratorCurr, txn, it);
        TxResponse* rsp = _rsp.consume();
        ASSERT(rspCode == rsp->rspCode);
    }

    void getIteratorGetNext(OperationContext* txn, const TxRsp& rspCode, RecordIterator* it) {
        _cmd.post(kIteratorGetNext, txn, it);
        TxResponse* rsp = _rsp.consume();
        ASSERT(rspCode == rsp->rspCode);
    }

    void getIteratorDataFor(OperationContext* txn, const TxRsp& rspCode, RecordIterator* it,
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
                    _rsp.post(kSucceeded, _rs->dataFor(req->txn, req->loc));
                    break;
                case kDelete:
                    _rs->deleteRecord(req->txn, req->loc);
                    _rsp.post(kSucceeded);
                    break;
                case kInsert:
                {
                    StatusWith<DiskLoc> res = _rs->insertRecord(req->txn, req->data, req->len, req->boolVal);
                    _rsp.post(kSucceeded, res.getStatus(), res.getValue());
                }
                    break;
                case kUpdateMove:
                {
                    StatusWith<DiskLoc> res = _rs->updateRecord(req->txn, req->loc, req->data, req->len,
                                                                req->boolVal, NULL);
                    _rsp.post(kSucceeded, res.getStatus(), res.getValue());
                }
                    break;
                case kUpdateInPlace:
                    _rsp.post(kSucceeded, _rs->updateWithDamages(req->txn, req->loc, req->data, req->damages));
                    break;
                case kGetIterator:
                    _rsp.post(kSucceeded, _rs->getIterator(req->txn, req->loc, req->boolVal, req->dir));
                    break;
                case kIteratorEof:
                    _rsp.post(kSucceeded, req->it->isEOF());
                    break;
                case kIteratorCurr:
                    _rsp.post(kSucceeded, req->it->curr());
                    break;
                case kIteratorGetNext:
                    _rsp.post(kSucceeded, req->it->getNext());
                    break;
                case kIteratorDataFor:
                    _rsp.post(kSucceeded, req->it->dataFor(req->loc));
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
    TxCommandBuffer _cmd;
    TxResponseBuffer _rsp;
    RecordStore *_rs;
    boost::thread _thr;
};

// captures ongoing state of a test run
// can only access disk locs that are in the Context
class Context {
private:
    map<DiskLoc,int> locMap;
    vector<DiskLoc> locVec;
    vector<RecordData> data;
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

class Operation {
    unsigned getTxn() const { return txn; }
private:
    unsigned txn;
    TxRequest req;
    TxResponse rsp;
};

typedef vector<Operation> Schedule;
typedef vector<Schedule> Workload;

class WorkLoadDescription {
public:
    Workload generate() const {
        Workload result;
        PseudoRandom rnd(time(0));

        for (int ix=0; ix < numBulkLoaders; ix++) {
            Schedule nextSchedule;
            result.push_back(nextSchedule);
        }

        for (int ix=0; ix < numCollectionScanners; ix++) {
            Schedule nextSchedule;
            result.push_back(nextSchedule);
        }

        for (int ix=0; ix < numPointModifiers; ix++) {
            Schedule nextSchedule;
            result.push_back(nextSchedule);
        }

        for (int ix=0; ix < numCollectionModifiers; ix++) {
            Schedule nextSchedule;
            result.push_back(nextSchedule);
        }
        return result;
    }
    
private:
    unsigned initialNumDocs;
    unsigned numBulkLoaders;
    unsigned numCollectionScanners;
    unsigned numPointReaders;
    unsigned numPointModifiers;
    unsigned numCollectionModifiers;
};

class WorkLoadScheduler {
public:
    WorkLoadScheduler(const Workload& workload) : _workload(workload) { }

    Schedule scheduleRandomly() {
        Schedule result;
        vector<ScheduleState> state;
        for (int ix = 0; ix < _workload.size(); ++ix) {
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
        for (int ix = 0; ix < _workload.size(); ++ix) {
            state.push_back(ScheduleState(_workload[ix].begin(), _workload[ix].end()));
        }
        
        return result;
    }
private:
    class ScheduleState {
    public:
        ScheduleState(Schedule::iterator next, const Schedule::iterator& last)
            : _next(next), _last(last) { }
        operator bool() const {return _next != _last;}
        Operation operator*() const {return *_next;}
        void operator++() {_next++;}
            
    private:
        Schedule::iterator _next;
        const Schedule::iterator& _last;
    };

    const Workload& _workload;
};

void execute(const WorkLoadDescription& wld, const vector<Operation>& schedule) {
    vector<ClientTransaction*> workers;
    for(unsigned ix=0; ix < wld.getNumWorkers(); ++ix) {
        workers.push_back(new ClientTransaction(wld.getRecordStore()));
    }
    for(Schedule::iterator it = schedule.begin(); it != schedule.end(); ++it) {
        try {
            Operation& op = *it;
            workers[op.getTxn()]->?invoke?(op.req);
        } catch (const LockManager::AbortException& err) {
            workers[(*it).getTxn()]->abort();
        }
    }
    for (unsigned ix=0; ix < wld.getNumWorkers(); ++ix) {
        ClientTransaction* nextWorker = workers[op.getTxn()];
        if (nextWorker->isAlive()) {
            nextWorker->quit();
        }
        delete nextWorker;
    }
}

void driver(const WorkLoadDescription& wld) {
    
    WorkLoadScheduler wls(wld.generate());

    if (true) {
        for (int ix=0; ix < NUM_RANDOM_TRIES; ++ix) {
            execute(wld, wls.scheduleRandomly());
        }
    }
    else {
        set<Schedule> schedules = wls.scheduleExhaustively();
        for (set<Schedule>::iterator nextSchedule = schedules.begin();
             nextSchedule != schedules.end(); ++nextSchedule) {
            execute(wld, *nextSchedule);
        }
    }
}

TEST(RecordStoreTest, RSBasic) {
}

} // namespace mongo
