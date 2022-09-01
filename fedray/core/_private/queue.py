import copyreg
import threading
import pickle
from queue import Queue as _Queue

# Make Queue a new-style class, so it can be used with copy_reg
class Queue(_Queue, object):
    pass

def pickle_queue(q):
    q_dct = q.__dict__.copy()
    del q_dct['mutex']
    del q_dct['not_empty']
    del q_dct['not_full']
    del q_dct['all_tasks_done']
    return Queue, (), q_dct

def unpickle_queue(state):
    q = state[0]()
    q.mutex = threading.Lock()
    q.not_empty = threading.Condition(q.mutex)
    q.not_full = threading.Condition(q.mutex)
    q.all_tasks_done = threading.Condition(q.mutex)
    q.__dict__ = state[2]
    return q

copyreg.pickle(Queue, pickle_queue, unpickle_queue)