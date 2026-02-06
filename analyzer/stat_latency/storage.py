import sqlite3
import threading
import json
import time
from queue import Queue, Empty
from typing import Iterator, Dict, List, Optional
from loguru import logger


class SqliteStorage:
    """Thread-safe sqlite-backed storage with a single background writer thread.

    Writes are enqueued and performed by the background writer in batches. Readers call
    `flush()` to ensure visibility of recent writes before performing reads.
    """

    def __init__(
        self,
        path: str = ":memory:",
        batch_size: int = 100,
        commit_threshold: int = 10000,
        wal: bool = True,
        synchronous: str = "NORMAL",
        cache_size: int = 60000,
    ):
        self.path = path
        self.batch_size = int(batch_size)
        self.commit_threshold = int(commit_threshold)
        self.wal = bool(wal)
        self.synchronous = synchronous
        self.cache_size = int(cache_size)

        logger.debug("SqliteStorage: initializing DB at {}", path)
        # allow connection use from multiple threads; the writer thread does all DB writes
        self.conn = sqlite3.connect(path, timeout=30, check_same_thread=False)
        # small lock used around reads and occasional commits
        self._lock = threading.RLock()

        with self._lock:
            # PRAGMA tuning
            if self.wal:
                try:
                    self.conn.execute("PRAGMA journal_mode=WAL;")
                except Exception:
                    logger.exception("SqliteStorage: failed to set WAL")
            try:
                self.conn.execute(f"PRAGMA synchronous={self.synchronous};")
            except Exception:
                logger.exception("SqliteStorage: failed to set synchronous")
            try:
                self.conn.execute(f"PRAGMA cache_size={self.cache_size};")
            except Exception:
                logger.exception("SqliteStorage: failed to set cache_size")
            self._init_schema()

        # queue for write operations
        self._op_queue: Queue = Queue()
        self._stop_event = threading.Event()
        self._writer_thread = threading.Thread(target=self._writer_loop, name="SqliteStorageWriter", daemon=True)
        self._writer_thread.start()

        # insert counter since last commit (used inside writer)
        self._insert_count = 0

    def _ensure_conn_open(self):
        """Try to ensure the sqlite connection is open; reopen if it has been closed unexpectedly."""
        try:
            # a light-weight check
            with self._lock:
                self.conn.execute("SELECT 1")
        except Exception as e:
            logger.warning("SqliteStorage: DB connection appears closed, attempting to reopen: {}", e)
            try:
                new_conn = sqlite3.connect(self.path, timeout=30, check_same_thread=False)
                # reapply tuning
                if self.wal:
                    try:
                        new_conn.execute("PRAGMA journal_mode=WAL;")
                    except Exception:
                        pass
                try:
                    new_conn.execute(f"PRAGMA synchronous={self.synchronous};")
                except Exception:
                    pass
                try:
                    new_conn.execute(f"PRAGMA cache_size={self.cache_size};")
                except Exception:
                    pass
                # do not reinit schema (it should already exist on disk)
                with self._lock:
                    try:
                        old = self.conn
                        self.conn = new_conn
                        try:
                            old.close()
                        except Exception:
                            pass
                        logger.info("SqliteStorage: reopened DB connection to {}", self.path)
                    except Exception:
                        logger.exception("SqliteStorage: failed to swap reopened connection")
            except Exception:
                logger.exception("SqliteStorage: failed to reopen DB connection")

    def _init_schema(self):
        c = self.conn.cursor()
        c.execute(
            """
        CREATE TABLE IF NOT EXISTS block_meta(
            hash TEXT PRIMARY KEY,
            txs INTEGER,
            size INTEGER,
            timestamp INTEGER,
            referees TEXT
        );
        """
        )

        c.execute(
            """
        CREATE TABLE IF NOT EXISTS block_latency(
            block_hash TEXT,
            lat_type TEXT,
            value REAL
        );
        """
        )
        c.execute("CREATE INDEX IF NOT EXISTS idx_block_lat ON block_latency(block_hash, lat_type);")

        c.execute(
            """
        CREATE TABLE IF NOT EXISTS tx_received(
            tx_hash TEXT,
            ts REAL
        );
        """
        )
        c.execute("CREATE INDEX IF NOT EXISTS idx_tx_received ON tx_received(tx_hash);")

        c.execute(
            """
        CREATE TABLE IF NOT EXISTS tx_packed(
            tx_hash TEXT,
            ts REAL
        );
        """
        )
        c.execute("CREATE INDEX IF NOT EXISTS idx_tx_packed ON tx_packed(tx_hash);")

        c.execute(
            """
        CREATE TABLE IF NOT EXISTS tx_ready(
            tx_hash TEXT,
            ts REAL
        );
        """
        )
        c.execute("CREATE INDEX IF NOT EXISTS idx_tx_ready ON tx_ready(tx_hash);")

        self.conn.commit()

    # -- Writer thread -------------------------------------------------
    def _writer_loop(self):
        logger.debug("SqliteStorage: writer thread started")
        batch = []
        while not self._stop_event.is_set():
            try:
                op = self._op_queue.get(timeout=0.5)
            except Empty:
                if batch:
                    self._process_batch(batch)
                    batch = []
                continue

            # sentinel to stop
            if op is None:
                self._op_queue.task_done()
                break

            batch.append(op)
            self._op_queue.task_done()

            if len(batch) >= self.batch_size:
                self._process_batch(batch)
                batch = []

        # flush remaining ops
        if batch:
            self._process_batch(batch)
        # drain queue if any left
        while True:
            try:
                op = self._op_queue.get_nowait()
            except Empty:
                break
            if op is not None:
                self._process_batch([op])
            self._op_queue.task_done()

        # final commit
        with self._lock:
            try:
                self.conn.commit()
            except Exception:
                logger.exception("SqliteStorage: final commit failed")
        logger.debug("SqliteStorage: writer thread exiting")

    def _process_batch(self, ops: List):
        if not ops:
            return
        with self._lock:
            cur = self.conn.cursor()
            try:
                for op in ops:
                    otype = op[0]
                    args = op[1]
                    if otype == "block_meta":
                        (block_hash, txs, size, timestamp, referees_json) = args
                        cur.execute(
                            "INSERT OR IGNORE INTO block_meta(hash, txs, size, timestamp, referees) VALUES (?,?,?,?,?)",
                            (block_hash, txs, size, timestamp, referees_json),
                        )
                        self._insert_count += 1
                    elif otype == "block_latency":
                        (block_hash, lat_type, value) = args
                        cur.execute(
                            "INSERT INTO block_latency(block_hash, lat_type, value) VALUES (?,?,?)",
                            (block_hash, lat_type, float(value)),
                        )
                        self._insert_count += 1
                    elif otype == "tx_received":
                        (tx_hash, ts) = args
                        cur.execute("INSERT INTO tx_received(tx_hash, ts) VALUES (?,?)", (tx_hash, float(ts)))
                        self._insert_count += 1
                    elif otype == "tx_packed":
                        (tx_hash, ts) = args
                        cur.execute("INSERT INTO tx_packed(tx_hash, ts) VALUES (?,?)", (tx_hash, float(ts)))
                        self._insert_count += 1
                    elif otype == "tx_ready":
                        (tx_hash, ts) = args
                        cur.execute("INSERT INTO tx_ready(tx_hash, ts) VALUES (?,?)", (tx_hash, float(ts)))
                        self._insert_count += 1
                    elif otype == "delete_block":
                        (block_hash,) = args
                        cur.execute("DELETE FROM block_latency WHERE block_hash = ?", (block_hash,))
                        cur.execute("DELETE FROM block_meta WHERE hash = ?", (block_hash,))
                        self._insert_count += 1
                    elif otype == "delete_tx":
                        (tx_hash,) = args
                        cur.execute("DELETE FROM tx_received WHERE tx_hash = ?", (tx_hash,))
                        cur.execute("DELETE FROM tx_packed WHERE tx_hash = ?", (tx_hash,))
                        cur.execute("DELETE FROM tx_ready WHERE tx_hash = ?", (tx_hash,))
                        self._insert_count += 1
                    else:
                        logger.warning("SqliteStorage: unknown op {}", otype)

                # commit when insert count reaches threshold
                if self._insert_count >= self.commit_threshold:
                    logger.debug("SqliteStorage: committing after {} ops", self._insert_count)
                    self.conn.commit()
                    self._insert_count = 0
                else:
                    # commit per batch to keep consistency; this is less frequent when commit_threshold is large
                    self.conn.commit()
            except Exception as e:
                logger.exception("SqliteStorage: processing batch failed: {}", e)
                try:
                    self.conn.rollback()
                except Exception:
                    logger.exception("SqliteStorage: rollback failed")

    # -- public write API (enqueue ops) -------------------------------
    def add_block_meta(self, block_hash: str, txs: int, size: int, timestamp: int, referees: List[str]):
        # logger.debug("SqliteStorage: enqueue add_block_meta {}", block_hash)
        self._op_queue.put(("block_meta", (block_hash, txs, size, timestamp, json.dumps(referees))))

    def add_block_latency(self, block_hash: str, lat_type: str, value: float):
        # logger.debug("SqliteStorage: enqueue add_block_latency {} {} {}", block_hash, lat_type, value)
        self._op_queue.put(("block_latency", (block_hash, lat_type, float(value))))

    def add_tx_received(self, tx_hash: str, ts: float):
        # logger.debug("SqliteStorage: enqueue add_tx_received {} {}", tx_hash, ts)
        self._op_queue.put(("tx_received", (tx_hash, float(ts))))

    def add_tx_packed(self, tx_hash: str, ts: float):
        self._op_queue.put(("tx_packed", (tx_hash, float(ts))))

    def add_tx_ready(self, tx_hash: str, ts: float):
        self._op_queue.put(("tx_ready", (tx_hash, float(ts))))

    def _maybe_commit(self, threshold: int = 1000):
        # no-op: commit policy handled by writer thread
        return

    def commit(self):
        # flush queue and ensure changes are committed
        self.flush()
        with self._lock:
            try:
                self.conn.commit()
            except Exception:
                logger.exception("SqliteStorage: commit failed")

    def flush(self, timeout: Optional[float] = None):
        """Block until queued write ops are processed."""
        # queue.join() waits until task_done() called for each enqueued item
        try:
            self._op_queue.join()
        except Exception:
            logger.exception("SqliteStorage: flush interrupted")

    # -- read API -----------------------------------------------------
    def iter_block_hashes(self) -> Iterator[str]:
        logger.debug("SqliteStorage: iter_block_hashes requested (flush first)")
        self.flush()
        self._ensure_conn_open()
        with self._lock:
            cur = self.conn.cursor()
            rows = [r[0] for r in cur.execute("SELECT hash FROM block_meta")]
        for h in rows:
            yield h

    def get_block_meta(self, block_hash: str) -> Optional[Dict]:
        self.flush()
        self._ensure_conn_open()
        with self._lock:
            cur = self.conn.cursor()
            cur.execute("SELECT txs, size, timestamp, referees FROM block_meta WHERE hash = ?", (block_hash,))
            row = cur.fetchone()
        if not row:
            return None
        txs, size, timestamp, referees = row
        return {"txs": txs, "size": size, "timestamp": timestamp, "referees": json.loads(referees)}

    def get_block_latencies(self, block_hash: str) -> Dict[str, List[float]]:
        self.flush()
        self._ensure_conn_open()
        with self._lock:
            cur = self.conn.cursor()
            cur.execute("SELECT lat_type, value FROM block_latency WHERE block_hash = ?", (block_hash,))
            rows = cur.fetchall()
        d = {}
        for lt, val in rows:
            d.setdefault(lt, []).append(val)
        return d

    def delete_block_raw(self, block_hash: str):
        self._op_queue.put(("delete_block", (block_hash,)))

    def iter_tx_hashes(self) -> Iterator[str]:
        logger.debug("SqliteStorage: iter_tx_hashes requested (flush first)")
        self.flush()
        self._ensure_conn_open()
        with self._lock:
            cur = self.conn.cursor()
            rows = [r[0] for r in cur.execute("SELECT DISTINCT tx_hash FROM (SELECT tx_hash FROM tx_received UNION SELECT tx_hash FROM tx_packed UNION SELECT tx_hash FROM tx_ready)")]
        for h in rows:
            yield h

    def get_tx_received(self, tx_hash: str) -> List[float]:
        self.flush()
        self._ensure_conn_open()
        with self._lock:
            cur = self.conn.cursor()
            cur.execute("SELECT ts FROM tx_received WHERE tx_hash = ?", (tx_hash,))
            rows = cur.fetchall()
        return [r[0] for r in rows]

    def get_tx_packed(self, tx_hash: str) -> List[float]:
        self.flush()
        self._ensure_conn_open()
        with self._lock:
            cur = self.conn.cursor()
            cur.execute("SELECT ts FROM tx_packed WHERE tx_hash = ?", (tx_hash,))
            rows = cur.fetchall()
        return [r[0] for r in rows]

    def get_tx_ready(self, tx_hash: str) -> List[float]:
        self.flush()
        self._ensure_conn_open()
        with self._lock:
            cur = self.conn.cursor()
            cur.execute("SELECT ts FROM tx_ready WHERE tx_hash = ?", (tx_hash,))
            rows = cur.fetchall()
        return [r[0] for r in rows]

    def delete_tx_raw(self, tx_hash: str):
        self._op_queue.put(("delete_tx", (tx_hash,)))

    def close(self):
        logger.debug("SqliteStorage: close() called; signaling writer to stop")
        # signal writer to stop
        self._op_queue.put(None)
        # wait until writer thread exits
        self._writer_thread.join(timeout=30)
        with self._lock:
            try:
                self.conn.commit()
            except Exception:
                pass
            try:
                self.conn.close()
            except Exception:
                pass
        logger.debug("SqliteStorage: close() finished")
