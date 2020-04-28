import random
import json
import pickle
from datetime import datetime, timezone

item = [x for x in range(15)]


def log_trans(trans):
    f = open(".\\log\\transaction_generated.txt", "a")
    f.write(str(datetime.now(timezone.utc)) + " " + str(trans) + "\n")
    f.close()


def helper(min_trans, max_trans):
    """
    generate a transaction with random number of
    read and write set within min_trans and max_trans,
    and log it

    :return: transaction
    eg, T1 = [start_time, end_time(-1 if unfinished), (0, 1, 2, 3), ((1, 15), (2, 20), (3, 30))]
    """
    num = random.randint(min_trans, max_trans)
    read_set = tuple(random.sample(item, num))
    num = random.randint(1, max_trans)
    write_set = random.sample(item, num)
    write_set = tuple([(x, random.randint(1, 100)) for x in write_set])
    transaction = [datetime.now(timezone.utc), -1, read_set, write_set]
    log_trans(transaction)
    return transaction


def random_transaction():
    """
    :return: Totally random transaction involving any number of items in database
    """
    return helper(0, 5)


def random_long_transaction():
    """
    :return: Transactions with 5-10 read and write items
    """
    return helper(3, 5)


def random_short_transaction():
    """
    :return: Transactions with no more than than 3 read and write item
    """
    return helper(0, 3)


# print(random_transaction())
# print(random_long_transaction())
# print(random_short_transaction())
# print(json.dumps(random_transaction()))
# a = pickle.dumps(random_transaction())
# b = pickle.loads(a)
# print(b[0] < datetime.now(timezone.utc))

