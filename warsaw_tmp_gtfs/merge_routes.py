from collections import defaultdict
from typing import cast

from impuls.task import Task, TaskRuntime


class MergeRoutes(Task):
    def __init__(self) -> None:
        super().__init__()

    def execute(self, r: TaskRuntime) -> None:
        with r.db.transaction():
            # Group all routes by the short_name
            routes_by_short_name = defaultdict[str, list[str]](list)
            for route_id, short_name in r.db.raw_execute("SELECT route_id, short_name FROM routes"):
                routes_by_short_name[cast(str, short_name)].append(cast(str, route_id))

            # Ensure only a single ID covers each short_name
            for short_name, ids in routes_by_short_name.items():
                r.db.raw_execute(
                    "UPDATE routes SET route_id = ? WHERE route_id = ?",
                    (short_name, ids[0]),
                )
                r.db.raw_execute_many(
                    "UPDATE trips SET route_id = ? WHERE route_id = ?",
                    ((short_name, id) for id in ids[1:]),
                )
                r.db.raw_execute_many(
                    "DELETE FROM routes WHERE route_id = ?",
                    ((id,) for id in ids[1:]),
                )
