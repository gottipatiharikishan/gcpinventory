"""Shared helpers for forward (GCS->Azure) and rollback (Azure->GCS) pipelines."""
import os, socket, subprocess, tempfile, time, logging
import psycopg2

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("sync")

WORKER_NAME = os.getenv("WORKER_NAME", socket.gethostname())
MAX_RETRY = int(os.getenv("MAX_RETRY", "5"))
BATCH_MAX_FILES = int(os.getenv("BATCH_MAX_FILES", "5000"))
BATCH_MAX_BYTES = int(os.getenv("BATCH_MAX_BYTES", str(5 * 1024 ** 3)))
BATCH_MAX_AGE_SEC = int(os.getenv("BATCH_MAX_AGE_SEC", "120"))
POLL_SEC = int(os.getenv("POLL_SEC", "10"))
RCLONE_FLAGS = os.getenv(
    "RCLONE_FLAGS", "--transfers=8 --checkers=8 --multi-thread-streams=4 --buffer-size=32M --retries=3"
).split()


def db():
    return psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"), dbname=os.getenv("PGDATABASE", "gcssync"),
        user=os.getenv("PGUSER", "gcssync"), password=os.environ["PGPASSWORD"],
    )


# Forward and rollback differ only by table/column names and rclone direction.
FORWARD = dict(
    events="file_events", batches="sync_batches", map="batch_files", map_col="file_event_id",
    group_col="bucket_name", name_col="object_name", size_col="size_bytes",
    src_remote=os.getenv("GCS_REMOTE", "gcs"), dst_remote=os.getenv("AZURE_REMOTE", "azure"),
)
ROLLBACK = dict(
    events="blob_events", batches="rollback_batches", map="rollback_batch_files", map_col="blob_event_id",
    group_col="container_name", name_col="blob_name", size_col=None,
    src_remote=os.getenv("AZURE_REMOTE", "azure"), dst_remote=os.getenv("GCS_REMOTE", "gcs"),
)


def create_batches(cfg):
    """Group NEW events per bucket/container into PENDING batches (count OR size OR age)."""
    ev, bt, mp, gc = cfg["events"], cfg["batches"], cfg["map"], cfg["group_col"]
    size_expr = f"COALESCE(SUM({cfg['size_col']}),0)" if cfg["size_col"] else "0"
    conn = db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT {gc}, COUNT(*), {size_expr}, EXTRACT(EPOCH FROM NOW()-MIN(created_at)) "
                f"FROM {ev} WHERE status='NEW' GROUP BY {gc}")
            groups = cur.fetchall()
        conn.commit()
        for grp, cnt, size, age in groups:
            if not (cnt >= BATCH_MAX_FILES or size >= BATCH_MAX_BYTES or age >= BATCH_MAX_AGE_SEC):
                continue
            while True:  # drain this group in capped batches
                with conn, conn.cursor() as cur:
                    cols = f"event_id, {cfg['size_col']}" if cfg["size_col"] else "event_id, 0"
                    cur.execute(
                        f"SELECT {cols} FROM {ev} WHERE status='NEW' AND {gc}=%s "
                        f"ORDER BY event_id LIMIT %s FOR UPDATE SKIP LOCKED", (grp, BATCH_MAX_FILES))
                    rows, ids, total = cur.fetchall(), [], 0
                    for i, s in rows:
                        ids.append(i); total += s or 0
                        if total >= BATCH_MAX_BYTES:
                            break
                    if not ids:
                        break
                    cur.execute(f"INSERT INTO {bt} ({gc},file_count,status) VALUES (%s,%s,'PENDING') RETURNING batch_id",
                                (grp, len(ids)))
                    bid = cur.fetchone()[0]
                    cur.executemany(f"INSERT INTO {mp} (batch_id,{cfg['map_col']}) VALUES (%s,%s)",
                                    [(bid, i) for i in ids])
                    cur.execute(f"UPDATE {ev} SET status='BATCHED' WHERE event_id = ANY(%s)", (ids,))
                    log.info("created batch %s for %s with %s files", bid, grp, len(ids))
                if len(ids) < BATCH_MAX_FILES and total < BATCH_MAX_BYTES:
                    break
    finally:
        conn.close()


def claim_batch(conn, cfg):
    with conn, conn.cursor() as cur:
        cur.execute(
            f"UPDATE {cfg['batches']} SET status='PROCESSING', started_at=NOW() WHERE batch_id = ("
            f"SELECT batch_id FROM {cfg['batches']} WHERE status='PENDING' "
            f"ORDER BY batch_id FOR UPDATE SKIP LOCKED LIMIT 1) RETURNING batch_id, {cfg['group_col']}")
        return cur.fetchone()


def process_batch(conn, cfg, batch_id, group):
    ev, bt, mp, nc = cfg["events"], cfg["batches"], cfg["map"], cfg["name_col"]
    with conn, conn.cursor() as cur:
        cur.execute(f"SELECT e.event_id, e.{nc} FROM {ev} e JOIN {mp} m ON m.{cfg['map_col']}=e.event_id "
                    f"WHERE m.batch_id=%s", (batch_id,))
        files = cur.fetchall()
    ids = [f[0] for f in files]
    # Remote container/bucket names are identical on both sides; override here if yours differ.
    with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False) as tf:
        tf.write("\n".join(f[1] for f in files)); list_path = tf.name
    cmd = ["rclone", "copy", f"{cfg['src_remote']}:{group}", f"{cfg['dst_remote']}:{group}",
           "--files-from", list_path, "--log-level", "INFO", *RCLONE_FLAGS]
    log.info("batch %s: %s files, %s", batch_id, len(files), " ".join(cmd))
    try:
        res = subprocess.run(cmd, capture_output=True, text=True)
        err = None if res.returncode == 0 else (res.stderr or "rclone failed")[-2000:]
    finally:
        os.unlink(list_path)

    with conn, conn.cursor() as cur:
        if err is None:
            cur.execute(f"UPDATE {ev} SET status='COMPLETED', processed_at=NOW(), worker_name=%s, "
                        f"error_message=NULL WHERE event_id = ANY(%s)", (WORKER_NAME, ids))
            cur.execute(f"UPDATE {bt} SET status='COMPLETED', completed_at=NOW() WHERE batch_id=%s", (batch_id,))
            log.info("batch %s COMPLETED", batch_id)
            return
        cur.execute(f"UPDATE {ev} SET retry_count=retry_count+1, worker_name=%s, error_message=%s "
                    f"WHERE event_id = ANY(%s)", (WORKER_NAME, err, ids))
        cur.execute(f"UPDATE {ev} SET status='FAILED' WHERE event_id = ANY(%s) AND retry_count >= %s", (ids, MAX_RETRY))
        cur.execute(f"UPDATE {ev} SET status='NEW' WHERE event_id = ANY(%s) AND status='BATCHED'", (ids,))
        cur.execute(f"SELECT COUNT(*) FROM {ev} WHERE event_id = ANY(%s) AND status='FAILED'", (ids,))
        permanently_failed = cur.fetchone()[0]
        # RETRIED = files went back to NEW for a new batch; FAILED = some hit the retry limit.
        cur.execute(f"UPDATE {bt} SET status=%s, completed_at=NOW() WHERE batch_id=%s",
                    ("FAILED" if permanently_failed else "RETRIED", batch_id))
        log.error("batch %s failed: %s", batch_id, err[:300])


def run_worker(cfg):
    conn = db()

    while True:
        try:
            claimed = claim_batch(conn, cfg)

            log.info("claimed=%s", claimed)

            if not claimed:
                time.sleep(POLL_SEC)
                continue

            log.info("starting batch=%s", claimed[0])

            process_batch(conn, cfg, *claimed)

            log.info("completed batch=%s", claimed[0])

        except psycopg2.Error:
            log.exception("db error, reconnecting")

            time.sleep(POLL_SEC)

            try:
                conn.close()
            except Exception:
                pass

            conn = db()

        except Exception:
            log.exception("worker error")

            time.sleep(POLL_SEC)
