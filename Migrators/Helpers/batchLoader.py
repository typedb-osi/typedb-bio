from functools import partial
from multiprocessing.dummy import Pool as ThreadPool
from tqdm import tqdm

from typedb.client import TransactionType

def write_batches(session, queries, batch_size, num_threads):
    with ThreadPool(num_threads) as pool:
        with tqdm(total=len(queries)) as pbar:
            chunk_size = max(1, len(queries) // num_threads // batch_size // 10)
            for n in pool.imap_unordered(partial(write_batch, session), iter_batches(queries, batch_size), chunk_size):
                pbar.update(n)


def iter_batches(a, batch_size):
    for i in range(0, len(a), batch_size):
        yield a[i:i+batch_size]


def write_batch(session, queries):
    with session.transaction(TransactionType.WRITE) as tx:
        for query in queries:
            tx.query().insert(query)
        tx.commit()
    return len(queries)
