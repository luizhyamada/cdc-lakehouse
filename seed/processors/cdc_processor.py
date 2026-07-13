import json


class CDCProcessor:

    @staticmethod
    def process(message):

        event = json.loads(
            message.value().decode()
        )

        source = event.get("source", {})
        event["entity"] = source.get("table")
        event["operation"] = event.get("op")
        event["lsn"] = source.get("lsn")
        event["snapshot"] = source.get("snapshot")
        event["event_time"] = event.get("ts_ms")

        return event