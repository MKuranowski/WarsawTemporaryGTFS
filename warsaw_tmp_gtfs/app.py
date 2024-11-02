import argparse
from datetime import date

import impuls

from .extend_schedules import ExtendSchedules
from .fix_stops import FixStops, UpdateStopNames
from .ftp import FTPResource, ZTMFeedProvider
from .merge_routes import MergeRoutes
from .update_feed_info import UpdateFeedInfo
from .update_trip_headsigns import UpdateTripHeadsigns

GTFS_HEADERS = {
    "agency": (
        "agency_id",
        "agency_name",
        "agency_url",
        "agency_timezone",
        "agency_lang",
        "agency_phone",
    ),
    "feed_info": (
        "feed_publisher_name",
        "feed_publisher_url",
        "feed_lang",
        "feed_version",
    ),
    "calendar_dates": ("service_id", "date", "exception_type"),
    "stops": (
        "stop_id",
        "stop_name",
        "stop_lat",
        "stop_lon",
    ),
    "routes": (
        "route_id",
        "agency_id",
        "route_short_name",
        "route_long_name",
        "route_type",
        "route_color",
        "route_text_color",
    ),
    "trips": (
        "trip_id",
        "route_id",
        "service_id",
        "trip_headsign",
        "direction_id",
        "shape_id",
    ),
    "stop_times": (
        "trip_id",
        "stop_id",
        "stop_sequence",
        "arrival_time",
        "departure_time",
        "pickup_type",
        "drop_off_type",
        "shape_dist_traveled",
    ),
    "shapes": (
        "shape_id",
        "shape_pt_sequence",
        "shape_pt_lat",
        "shape_pt_lon",
        "shape_dist_traveled",
    ),
}


class WarsawTemporaryGTFS(impuls.App):
    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument("-k", "--apikey", help="api.um.warszawa.pl apikey")

    def prepare(
        self,
        args: argparse.Namespace,
        options: impuls.PipelineOptions,
    ) -> impuls.multi_file.MultiFile[FTPResource]:
        return impuls.multi_file.MultiFile(
            options=options,
            intermediate_provider=ZTMFeedProvider(),
            intermediate_pipeline_tasks_factory=lambda feed: [
                impuls.tasks.LoadGTFS(feed.resource_name),
                impuls.tasks.ExecuteSQL(
                    "DropNonZTMRoutes",
                    "DELETE FROM agencies WHERE agency_id = '5'",
                ),
                impuls.tasks.ExecuteSQL(
                    "FixAgencyData",
                    (
                        "UPDATE agencies SET name = 'Warszawski Transport Publiczny', "
                        "url = 'https://wtp.waw.pl', phone = '+48 22 19 115'"
                    ),
                ),
                impuls.tasks.ExecuteSQL(
                    "DropInaccessibleStopTimes",
                    "DELETE FROM stop_times WHERE pickup_type = 1 AND drop_off_type = 1",
                ),
                impuls.tasks.ExecuteSQL(
                    "DropUnusedStops",
                    (
                        "DELETE FROM stops WHERE location_type = 0 AND "
                        "NOT EXISTS (SELECT stop_id FROM stop_times WHERE "
                        "            stop_times.stop_id = stops.stop_id)"
                    ),
                ),
                MergeRoutes(),
                FixStops(),
                UpdateStopNames(),
                impuls.tasks.ExecuteSQL(
                    "UpdateRouteColors",
                    (
                        "UPDATE routes SET text_color = 'FFFFFF', color = CASE"
                        "  WHEN type = 2 THEN '009955'"
                        "  WHEN type = 0 THEN 'B60000'"
                        "  WHEN short_name LIKE 'N%' THEN '000000'"
                        "  WHEN short_name LIKE 'L%' THEN '000088'"
                        "  WHEN short_name LIKE '7%' THEN '006800'"
                        "  WHEN short_name LIKE '8%' THEN '006800'"
                        "  WHEN short_name LIKE '4%' THEN 'B60000'"
                        "  WHEN short_name LIKE '5%' THEN 'B60000'"
                        "  WHEN short_name LIKE 'E%' THEN 'B60000'"
                        "  ELSE '880077' END"
                    ),
                ),
                UpdateTripHeadsigns(),
                impuls.tasks.ExecuteSQL(
                    "MoveStopCodeToName",
                    (
                        "UPDATE stops SET name = concat(name, ' ', code), code = '' "
                        "WHERE code != '' AND SUBSTR(stop_id, 2, 2) NOT IN ('90', '91', '92') "
                        "      AND stop_id NOT LIKE '1930%'"
                    ),
                ),
                UpdateFeedInfo(feed.version),
            ],
            final_pipeline_tasks_factory=lambda _: [
                # XXX: This is a workaround for bug in Impuls's LoadGtfs;
                #      it loads the block_id as "" instead of NULL
                impuls.tasks.ExecuteSQL("RemoveBlockId", "UPDATE trips SET block_id = NULL"),
                ExtendSchedules(),
                impuls.tasks.SaveGTFS(GTFS_HEADERS, "warsaw.zip"),
            ],
            additional_resources={
                "stops.json": impuls.HTTPResource.get(
                    "https://api.um.warszawa.pl/api/action/dbstore_get/",
                    params={
                        "id": "ab75c33d-3a26-4342-b36a-6e5fef0a3ac3",
                        "apikey": args.apikey,
                    },
                ),
                "stops.html": impuls.HTTPResource.get(
                    "https://www.wtp.waw.pl/rozklady-jazdy/",
                    params={
                        "wtp_dt": date.today().isoformat(),
                        "wtp_md": "1",
                    },
                ),
                "extra_stop_groups.json": impuls.LocalResource("extra_stop_groups.json"),
                "calendar_exceptions.csv": impuls.tools.polish_calendar_exceptions.RESOURCE,
            },
        )
