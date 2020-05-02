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
import grpc
import pickle
from concurrent import futures
import grpc_dir.update_db_pb2 as update_db_pb2
import grpc_dir.update_db_pb2_grpc as update_db_pb2_grpc
import port

data = [0 for x in range(15)]  # Note Here, no lock on this data item! OPCC

db = mysql.connector.connect(
    host="localhost",
    port="3306",
    user="root",
    passwd="nopassword",
    database="opcc2"
)
cursor = db.cursor()

channel = grpc.insecure_channel('localhost:50051')
stub = global_validation_pb2_grpc.GlobalValidationStub(channel)

rejected_transaction_lock = threading.Lock()
rejected_transactions = []  # sorted , with the last one being rejected the most time
committed_transaction_lock = threading.Lock()
committed_transactions = []  # DCG, treat simply as a list, sorted by the end time of a transaction. #TODO should have globally naming
semi_committed_transaction_lock = threading.Lock()
semi_committed_transactions = set()
rank_condition = threading.Condition()
rank = 0


def log_rejected_transaction(trans, trans_type=""):
    f = open(".\\log\\rejected_transaction_site2.txt", "a")
    f.write(str(datetime.now(timezone.utc)) + " " + str(trans) + "\n" + trans_type + "\n")
    f.close()


def log_committed_transaction(trans, trans_type="", read_val=None):
    f = open(".\\log\\committed_transaction_site2.txt", "a")
    f.write(str(datetime.now(timezone.utc)) + " " + str(trans) + "\n" + trans_type + "\n" + "read_value:" + str(
        read_val) + "\n" + "db snap shot: " + str(data) + "\n")
    f.close()

# def log_db_snapshot(trans):
#     f = open(".\\log\\db_snapshot_site0.txt", "a")
#     f.write(str(trans) + "\n" + str(data) + "\n")
#     f.close()

def update_db_helper(trans):
    """
    write to db
    """
    write_set = trans[3]
    # sql = "UPDATE data SET value = %s WHERE name = %s"
    # val = write_set
    # cursor.executemany(sql, val)
    # db.commit()
    for item, value in write_set:
        data[item] = value
    return


def update_db(trans, assigned_rank, read_val=None, trans_type=""):
    global rank
    rank_condition.acquire()
    current_rank = rank
    while current_rank != assigned_rank:
        rank_condition.wait()
        current_rank = rank
    rank += 1
    update_db_helper(trans)
    rank_condition.notifyAll()
    log_committed_transaction(trans, trans_type, read_val)
    # log_db_snapshot(trans)
    rank_condition.release()


def validate_two_trans(cur_trans, prev_trans):
    cur_read_set = set(cur_trans[2])
    prev_write_set = set()
    for item in prev_trans[3]:
        prev_write_set.add(item[0])
    if cur_read_set.intersection(prev_write_set):
        return False
    return True


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


def global_validation(trans):
    """
    assume clock is well synced, called central site method for validate using grpc
    :param trans:
    :return:
    """
    return stub.GlobalValidate(global_validation_pb2.Transaction(transaction=pickle.dumps(trans), init_site=2))


def try_commit(trans, read_val, trans_type=""): #TODO add log here
    trans_tuple = tuple(trans)
    if not local_validation(trans):
        return False
    semi_committed_transaction_lock.acquire()
    semi_committed_transactions.add(trans_tuple)
    semi_committed_transaction_lock.release()
    global_result = global_validation(trans)
    semi_committed_transaction_lock.acquire()
    semi_committed_transactions.remove(trans_tuple)
    semi_committed_transaction_lock.release()
    # if global_result:
    #     update_db(trans, global_result.rank, read_val, trans_type)
    return global_result.result


def read_data(trans):
    read_set = trans[2]
    read_val = []
    for i in read_set:
        read_val.append(data[i])
    return read_val


def process_long_transaction():
    # Timer Ahead, the incoming transactions just come regardless of your system loading
    threading.Timer(2, process_long_transaction).start()
    trans = transaction_gen.random_transaction()
    # manually add compute time of 1s for short transaction
    read_val = read_data(trans)
    time.sleep(3)
    result = try_commit(trans, read_val, "long_transaction")
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
        # log_committed_transaction(trans, "long transaction")
    return result


def process_short_transaction():
    # Timer Ahead, the incoming transactions just come regardless of your system loading
    threading.Timer(2, process_short_transaction).start()
    trans = transaction_gen.random_transaction()
    read_val = read_data(trans)
    # manually add compute time of 1s for short transaction
    time.sleep(1)
    result = try_commit(trans, read_val, "short_transaction")
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
        # log_committed_transaction(trans, "short transaction")
    return result


def redo_rejected_transaction():
    if rejected_transactions:
        rejected_transaction_lock.acquire()
        unlucky_trans = rejected_transactions.pop(0)
        rejected_transaction_lock.release()
        fail_time = unlucky_trans[0]
        trans = unlucky_trans[1]
        trans[0] = datetime.now(timezone.utc)
        read_val = read_data(trans)
        result = try_commit(trans, read_val, "redo transaction")
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
            # log_committed_transaction(trans, "redo transaction")
    # Timer Behind, always have only one thread solving the rejected transaction
    # if too much failed transaction in the fail log, put it ahead to have more
    # thread solving the rejected transactions
    threading.Timer(1, redo_rejected_transaction).start()


class UpdateDBServicer(update_db_pb2_grpc.UpdateDBServicer):
    """Provides methods that implement functionality of global validate server."""

    def UpdateDB(self, request, context):
        trans = pickle.loads(request.transaction)
        assigned_rank = request.rank
        update_db(trans, assigned_rank)
        return update_db_pb2.Empty()


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    update_db_pb2_grpc.add_UpdateDBServicer_to_server(
        UpdateDBServicer(), server)
    server.add_insecure_port('[::]:50054')  # TODO change for site1 and site2
    server.start()
    server.wait_for_termination()


def init():
    """
    bootstrap local site

    :return:
    """
    thread = threading.Thread(target=serve)
    thread.start()
    while not (port.isOpen("localhost", 50051) and port.isOpen("localhost", 50052) and port.isOpen("localhost", 50053) and port.isOpen("localhost", 50054)):
        print("port not open!")
    process_long_transaction()
    process_short_transaction()
    redo_rejected_transaction()
    # stub.GlobalValidate(global_validation_pb2.Transaction(transaction=pickle.dumps(transaction_gen.random_short_transaction()), init_site=0))


init()
