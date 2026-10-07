"""Replay dead-lettered GCS notifications: DLQ subscription -> main topic (gcs-events).
Acknowledges a DLQ message only after it was re-published. Run after fixing the root cause."""
import os
from google.cloud import pubsub_v1

project = os.environ["PUBSUB_SUBSCRIPTION"].split("/")[1]
sub, pub = pubsub_v1.SubscriberClient(), pubsub_v1.PublisherClient()
sub_path = sub.subscription_path(project, "gcs-dlq-sub")
topic_path = pub.topic_path(project, "gcs-events")

replayed = 0
while True:
    resp = sub.pull(request={"subscription": sub_path, "max_messages": 100}, timeout=30)
    if not resp.received_messages:
        break
    ack_ids = []
    for m in resp.received_messages:
        pub.publish(topic_path, m.message.data, **dict(m.message.attributes)).result()
        ack_ids.append(m.ack_id)
    sub.acknowledge(request={"subscription": sub_path, "ack_ids": ack_ids})
    replayed += len(ack_ids)
print("replayed", replayed)
