import json
import math
import re
from collections import defaultdict
from dataclasses import dataclass
from statistics import mean
from typing import Any

from impuls.db import DBConnection
from impuls.model import Stop
from impuls.task import Task, TaskRuntime

NO_POLISH_DIACRITICS_MAP = str.maketrans("ąćęłńóśźżĄĆĘŁŃÓŚŹŻ", "acelnoszzACELNOSZZ")

NAME_WORD_NORMALIZE = {
    "osiedle": "os",
    "dworzec": "dw",
    "cmentarz": "cm",
    "plac": "pl",
    "aleja": "al",
    "aleje": "al",
}


def slugify_name(name: str) -> str:
    words = re.findall(r"\w+", name.lower().translate(NO_POLISH_DIACRITICS_MAP))
    words = [NAME_WORD_NORMALIZE.get(word, word) for word in words]
    return "_".join(words)


@dataclass
class ExternalStop:
    id: str
    name: str
    lat: float
    lon: float


@dataclass
class ExternalStopGroup:
    id: str
    slug: str
    lat: float
    lon: float


class FixStops(Task):
    def __init__(self) -> None:
        super().__init__()
        self.external_stops_by_id = dict[str, ExternalStop]()
        self.external_stops_by_position = defaultdict[tuple[float, float], list[ExternalStop]](list)
        self.external_groups_by_slug = defaultdict[str, list[ExternalStopGroup]](list)
        self.seen_ids = set[str]()

    def execute(self, r: TaskRuntime) -> None:
        self.seen_ids.clear()
        self.load_external_data(
            stops=r.resources["stops.json"].json(),
            extra_groups=r.resources["extra_stop_groups.json"].json(),
        )
        with r.db.transaction():
            r.db.raw_execute("UPDATE stops SET stop_id = concat('_gtfs_', stop_id)")
            stops = list(r.db.retrieve_all(Stop))
            for stop in stops:
                self.process_stop(stop, r.db)

    def process_stop(self, stop: Stop, db: DBConnection) -> None:
        # Update stop ID
        fixed_id = self.match_stop(stop)
        if fixed_id:
            if fixed_id in self.seen_ids:
                self.logger.warning(
                    "Multiple instances of stop %s (%s %s)",
                    fixed_id,
                    stop.name,
                    stop.code,
                )
                db.raw_execute(
                    "UPDATE stop_times SET stop_id = ? WHERE stop_id = ?",
                    (fixed_id, stop.id),
                )
                db.raw_execute("DELETE FROM stops WHERE stop_id = ?", (stop.id,))
            else:
                db.raw_execute(
                    "UPDATE stops SET stop_id = ? WHERE stop_id = ?",
                    (fixed_id, stop.id),
                )
                stop.id = fixed_id
                self.seen_ids.add(fixed_id)

    def load_external_data(self, stops: Any, extra_groups: Any) -> None:
        self.load_external_stops_by_id(stops)
        # self.load_external_stops_by_position()
        self.load_external_groups_by_slug(extra_groups)

    def load_external_stops_by_id(self, stops: Any) -> None:
        self.external_stops_by_id.clear()
        for raw_obj in stops["result"]:
            obj = {i["key"]: i["value"] for i in raw_obj["values"]}
            id = obj["zespol"] + obj["slupek"]
            self.external_stops_by_id[id] = ExternalStop(
                id=id,
                name=obj["nazwa_zespolu"],
                lat=float(obj["szer_geo"]),
                lon=float(obj["dlug_geo"]),
            )

    def load_external_stops_by_position(self) -> None:
        self.external_stops_by_position.clear()
        for stop in self.external_stops_by_id.values():
            pos = round(stop.lat, 6), round(stop.lon, 6)
            self.external_stops_by_position[pos].append(stop)

    def load_external_groups_by_slug(self, extra_groups: Any) -> None:
        stops_by_group_id = defaultdict[str, list[ExternalStop]](list)
        for stop in self.external_stops_by_id.values():
            stops_by_group_id[stop.id[:4]].append(stop)

        groups = [ExternalStopGroup(**i) for i in extra_groups]
        for id, stops in stops_by_group_id.items():
            groups.append(
                ExternalStopGroup(
                    id=id,
                    slug=slugify_name(stops[0].name),
                    lat=mean(i.lat for i in stops),
                    lon=mean(i.lon for i in stops),
                )
            )

        self.external_groups_by_slug.clear()
        for group in groups:
            self.external_groups_by_slug[group.slug].append(group)

    def match_stop(self, stop: Stop) -> str:
        if not re.match(r"^[0-9][0-9]$", stop.code):
            self.logger.error(
                "Stop %s (%s %s) has invalid code: %r",
                stop.id,
                stop.name,
                stop.code,
                stop.code,
            )
            return ""

        # Try to match the stop based on its position
        # stop_matches = self.external_stops_by_position.get((round(stop.lat, 6), round(stop.lon, 6)))
        # if stop_matches and len(stop_matches) == 1:
        #     # Unique match - return the external stop and its id
        #     id = stop_matches[0].id[:4] + stop.code
        #     if id != stop_matches[0].id:
        #         self.logger.warning(
        #             "Stop %s (%s %s) matched uniquely with a stop with a different code: %s (%s)",
        #             stop.id,
        #             stop.name,
        #             stop.code,
        #             stop_matches[0].id,
        #             stop_matches[0].name,
        #         )
        #     return id
        # elif stop_matches and all(i.id[:4] == stop_matches[0].id[:4] for i in stop_matches[1:]):
        #     # Non-unique match, but all matches belong to the same group -
        #     # use the group id with stop.code as the fixed id
        #     id = stop_matches[0].id[:4] + stop.code
        #     return id

        # Try to match the group based on the name, resolving conflicts based on the position
        group_matches = self.external_groups_by_slug.get(slugify_name(stop.name))
        if group_matches and len(group_matches) == 1:
            # Unique group match - use the group id with stop.code as the fixed id
            id = group_matches[0].id + stop.code
            return id
        elif group_matches:
            # Non-unique group match - resolve conflict by picking the closest group
            group = min(
                group_matches,
                key=lambda g: math.dist((g.lat, g.lon), (stop.lat, stop.lon)),
            )
            id = group.id + stop.code
            return id

        # Unable to match :^(
        self.logger.warning(
            "Failed to match stop %s (%s %s) with external data",
            stop.id,
            stop.name,
            stop.code,
        )
        return ""


class UpdateStopNames(Task):
    def __init__(self) -> None:
        super().__init__()
        self.group_id_to_town_name = dict[str, str]()

    def execute(self, r: TaskRuntime) -> None:
        self.load_group_to_town_name_mapping(r.resources["stops.html"].text(encoding="utf-8"))
        with r.db.transaction():
            r.db.raw_execute_many(
                "UPDATE stops SET name = concat(?, ' ', name) WHERE substr(stop_id, 1, 4) = ?",
                (
                    (town_name, group_id)
                    for group_id, town_name in self.group_id_to_town_name.items()
                ),
            )

    def load_group_to_town_name_mapping(self, website_content: str) -> None:
        data_match = re.search(r"document.wtpTimetableStopsEncoded\s*=\s*'(.+)';", website_content)
        if not data_match:
            raise ValueError("Failed to extract document.wtpTimetableStopsEncoded from stops.html")
        self.group_id_to_town_name = {
            group["id"]: group["city"].title()
            for group in json.loads(data_match.group(1))
            if self.should_town_name_be_added_to_stop_name(
                group["id"],
                group["name"],
                group["city"],
                group["city_code"],
            )
        }

    @staticmethod
    def should_town_name_be_added_to_stop_name(
        id: str,
        name: str,
        town: str,
        town_code: str,
    ) -> bool:
        # No for stops in Warsaw
        if town_code == "--":
            return False

        # No for railway stations
        if id[1:3] in {"90", "91", "92"} or id == "1930":
            return False

        # No for stops close to railway stations
        name = name.casefold()
        if "pkp" in name or "wkd" in name:
            return False

        # No for stops already containing the town name
        town = town.casefold()
        if town in name:
            return False

        # No if the stop and town names intersect
        if any(part in name for part in town.split()):
            return False

        return True
