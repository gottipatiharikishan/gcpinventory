"""Azure Storage Queue (Event Grid BlobCreated) -> PostgreSQL blob_events."""

import base64
import json
import os
import time

from azure.storage.queue import QueueClient

from common import db, log


queue = QueueClient.from_connection_string(
    os.environ["AZURE_QUEUE_CONN_STR"],
    os.environ["AZURE_QUEUE_NAME"]
)


def parse(content):
    try:
        data = json.loads(content)
    except ValueError:
        data = json.loads(base64.b64decode(content))

    return data if isinstance(data, list) else [data]


def main():

    while True:

        got = False

        conn = db()

        try:

            for m in queue.receive_messages(
                messages_per_page=32,
                visibility_timeout=120
            ):

                got = True

                try:

                    with conn:
                        with conn.cursor() as cur:

                            for ev in parse(m.content):

                                event_type = ev.get("eventType", "")

                                if "BlobCreated" not in event_type:
                                    continue

                                container, blob = (
                                    ev["subject"]
                                    .split("/containers/", 1)[1]
                                    .split("/blobs/", 1)
                                )

                                event_id = (
                                    f"{container}:{blob}:{event_type}"
                                )

                                cur.execute(
                                    """
                                    INSERT INTO blob_events
                                    (
                                        event_id,
                                        container_name,
                                        blob_name,
                                        event_type,
                                        status
                                    )
                                    VALUES
                                    (
                                        %s,
                                        %s,
                                        %s,
                                        %s,
                                        'NEW'
                                    )
                                    ON CONFLICT (event_id)
                                    DO NOTHING
                                    """,
                                    (
                                        event_id,
                                        container,
                                        blob,
                                        event_type
                                    )
                                )

                    queue.delete_message(m)

                except Exception:

                    log.exception(
                        "failed message %s (will reappear after visibility timeout)",
                        m.id
                    )

            if not got:
                time.sleep(
                    int(os.getenv("POLL_SEC", "5"))
                )

        finally:
            conn.close()


if __name__ == "__main__":
    main()
