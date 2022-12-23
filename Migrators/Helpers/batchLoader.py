from functools import partial
from multiprocessing.dummy import Pool as ThreadPool
from tqdm import tqdm

from typedb.client import TransactionType

def write_batches(session, batches, num_threads):
    with ThreadPool(num_threads) as pool:
        with tqdm(total=len(batches)) as pbar:
            for _ in pool.imap_unordered(partial(write_batch, session), batches, max(1, len(batches) // num_threads // 10)):
                pbar.update()

def write_batch(session, batch):
    with session.transaction(TransactionType.WRITE) as tx:
        for query in batch:
            tx.query().insert(query)
        tx.commit()
