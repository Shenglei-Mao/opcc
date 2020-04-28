# run on local site
# functions including:
# 1.generate transaction (add compute time),
# 2.maintain all the data structure,
# 3.validate transaction,
# 4.send semi-commit transaction for global validation
# 5.abort transaction
import threading
import time
import bisect
from datetime import datetime, timezone
import transaction_gen
import mysql.connector
import grpc_dir.global_validation_pb2 as global_validation_pb2
import grpc_dir.global_validation_pb2_grpc as global_validation_pb2_grpc
import pickle
from concurrent import futures
import grpc
import grpc_dir.update_db_pb2 as update_db_pb2
import grpc_dir.update_db_pb2_grpc as update_db_pb2_grpc
import socket
import port

channel_0 = grpc.insecure_channel('localhost:50052')
stub_0 = update_db_pb2_grpc.UpdateDBStub(channel_0)
# channel_1 = grpc.insecure_channel('localhost:50053')
# stub_1 = update_db_pb2_grpc.UpdateDBStub(channel_1)
# channel_2 = grpc.insecure_channel('localhost:50054')
# stub_2 = update_db_pb2_grpc.UpdateDBStub(channel_2)
stubs = [stub_0, -1, -1]

db = mysql.connector.connect(
    host="localhost",
    port="3306",
    user="root",
    passwd="nopassword",
    database="opcc_central"
)
cursor = db.cursor()

rejected_transaction_lock = threading.Lock()
rejected_transactions = []  # sorted , with the last one being rejected the most time
committed_transaction_lock = threading.Lock()
committed_transactions = []  # DCG, treat simply as a list, sorted by the end time of a transaction. #TODO should have globally naming
semi_committed_transaction_lock = threading.Lock()
semi_committed_transactions = set()
rank_lock = threading.Lock()
rank = 0  # rank at global scope, rank token give out
rank_condition = threading.Condition()
# current_rank_lock = threading.Lock()
# current_rank = 0



def log_rejected_transaction_global(trans):
    f = open(".\\log\\rejected_transaction_central_at_global_validation_phase.txt", "a")
    f.write(str(datetime.now(timezone.utc)) + " " + str(trans) + "\n")
    f.close()


def log_rejected_transaction(trans, trans_type=""):
    f = open(".\\log\\rejected_transaction_central_for_transactions_init_at_central_at_either_phase.txt", "a")
    f.write(str(datetime.now(timezone.utc)) + " " + str(trans) + "\n" + trans_type + "\n")
    f.close()


def log_committed_transaction(trans, trans_type="", read_val=None):
    f = open(".\\log\\committed_transaction_central.txt", "a")
    f.write(str(datetime.now(timezone.utc)) + " " + str(trans) + "\n" + trans_type + "\n" + "read_value:" + str(read_val) + "\n")
    f.close()


def validate_two_trans(cur_trans, prev_trans):
    cur_read_set = set(cur_trans[2])
    prev_write_set = set()
    for item in prev_trans[3]:
        prev_write_set.add(item[0])
    if cur_read_set.intersection(prev_write_set):
        return False
    return True

def update_db_helper(trans):
    """
    write to db
    """
    write_set = trans[3]
    sql = "UPDATE data Set value = %s WHERE name = %s"
    val = write_set
    cursor.executemany(sql, val)
    # db.commit()

def update_db(trans, assigned_rank):
    # global rank
    # rank_condition.acquire()
    # current_rank = rank
    # while current_rank != assigned_rank:
    #     rank_condition.wait()
    #     current_rank = rank
    # rank += 1
    update_db_helper(trans)
    # rank_condition.notifyAll()
    # rank_condition.release()

def local_validation(trans):
    """
    local validate against 1. all the committed transaction that have end time later(strictly larger) than
    the start time of current transaction; 2. all the semi_committed_transactions
    For the above transactions x and this transaction y
    1. if the write set of x overlap with the read set y, y needs to redo it
    * 2. if x is a semi_committed_transaction, if x's write set overlap with the write set of y (not necessary)
    :return: Boolean
    """
    committed_transaction_lock.acquire()
    key_list = [x[1] for x in committed_transactions]
    idx = bisect.bisect(key_list, trans[0])
    for committed_transaction in committed_transactions[idx:]:
        if not validate_two_trans(trans, committed_transaction):
            committed_transaction_lock.release()  # DAMN, 1 hour debug for this dead lock
            return False
    committed_transaction_lock.release()
    semi_committed_transaction_lock.acquire()
    for semi_committed_transaction in semi_committed_transactions:
        if not validate_two_trans(trans, semi_committed_transaction):
            semi_committed_transaction_lock.release()  # Look up for the comment
            return False
    semi_committed_transaction_lock.release()
    return True


def global_validation(trans, site):
    """
    assume clock is well synced, called central site method for validate using grpc
    validate against all the global committed transaction & central site's semi-committed transaction
    """
    global rank
    if not local_validation(trans):
        log_rejected_transaction_global(trans)
        return global_validation_pb2.Result(result=False, rank=-1)
    else:
        rank_lock.acquire()
        assigned_rank = rank
        rank += 1
        update_db(trans, -1)
        rank_lock.release()
        for i in range(3):
            if i != site and stubs[i] != -1:
                # update_db(trans, rank) #rpc call
                stubs[i].UpdateDB(update_db_pb2.UpdateTransaction(transaction=pickle.dumps(trans), rank=assigned_rank))
        log_committed_transaction(trans, "remote transaction")
        return global_validation_pb2.Result(result=True, rank=assigned_rank)


class GlobalValidationServicer(global_validation_pb2_grpc.GlobalValidationServicer):
    """Provides methods that implement functionality of global validate server."""
    def GlobalValidate(self, request, context):
        trans = pickle.loads(request.transaction)
        site = request.init_site
        return global_validation(trans, site)


def try_commit(trans):
    trans_tuple = tuple(trans)
    if not local_validation(trans):
        return False
    semi_committed_transaction_lock.acquire()
    semi_committed_transactions.add(trans_tuple)
    semi_committed_transaction_lock.release()
    global_result = global_validation(trans, -1)
    semi_committed_transaction_lock.acquire()
    semi_committed_transactions.remove(trans_tuple)
    semi_committed_transaction_lock.release()
    if global_result:
        update_db(trans, global_result.rank)
    return global_result.result


def process_long_transaction():
    # Timer Ahead, the incoming transactions just come regardless of your system loading
    threading.Timer(2, process_long_transaction).start()
    trans = transaction_gen.random_transaction()
    # manually add compute time of 1s for short transaction
    time.sleep(3)  # TODO try random here
    result = try_commit(trans)
    if not result:
        rejected_transaction_lock.acquire()
        bisect.insort(rejected_transactions, (1, trans))
        rejected_transaction_lock.release()
        log_rejected_transaction(trans, "long transaction")
    else:
        committed_transaction_lock.acquire()
        trans[1] = datetime.now(timezone.utc)
        committed_transactions.append(trans)
        committed_transaction_lock.release()
        log_committed_transaction(trans, "long transaction")
    return result


def process_short_transaction():
    # Timer Ahead, the incoming transactions just come regardless of your system loading
    threading.Timer(2, process_short_transaction).start()
    trans = transaction_gen.random_transaction()
    # TODO add read and write
    # manually add compute time of 1s for short transaction
    time.sleep(1)  # TODO try random here
    result = try_commit(trans)
    if not result:
        rejected_transaction_lock.acquire()
        bisect.insort(rejected_transactions, (1, trans))
        rejected_transaction_lock.release()
        log_rejected_transaction(trans, "short transaction")
    else:
        committed_transaction_lock.acquire()
        trans[1] = datetime.now(timezone.utc)
        committed_transactions.append(trans)
        committed_transaction_lock.release()
        log_committed_transaction(trans, "short transaction")
    return result


def redo_rejected_transaction():
    if rejected_transactions:
        rejected_transaction_lock.acquire()
        unlucky_trans = rejected_transactions.pop()
        rejected_transaction_lock.release()
        fail_time = unlucky_trans[0]
        trans = unlucky_trans[1]
        trans[0] = datetime.now(timezone.utc)
        result = try_commit(trans)
        if not result:
            rejected_transaction_lock.acquire()
            bisect.insort(rejected_transactions, (fail_time + 1, trans))
            rejected_transaction_lock.release()
            log_rejected_transaction(trans)
        else:
            committed_transaction_lock.acquire()
            trans[1] = datetime.now(timezone.utc)
            committed_transactions.append(trans)
            committed_transaction_lock.release()
            log_committed_transaction(trans, "redo transaction")
    # Timer Behind, always have only one thread solving the rejected transaction
    # if too much failed transaction in the fail log, put it ahead to have more
    # thread solving the rejected transactions
    threading.Timer(1, redo_rejected_transaction).start()


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    global_validation_pb2_grpc.add_GlobalValidationServicer_to_server(
        GlobalValidationServicer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    server.wait_for_termination()


def init():
    """
    bootstrap local site
    :return:
    """
    thread = threading.Thread(target=serve)
    thread.start()
    while not (port.isOpen("localhost", 50051) and port.isOpen("localhost", 50052)):
        print("port not open!")
    process_long_transaction()
    process_short_transaction()
    redo_rejected_transaction()




init()