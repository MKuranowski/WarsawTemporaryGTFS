import csv
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Generator
from zipfile import ZIP_DEFLATED, ZipFile

from impuls.task import Task, TaskRuntime


class FixAgencyID(Task):
    def __init__(self, gtfs_resource: str) -> None:
        self.gtfs_resource = gtfs_resource
        super().__init__()

    def execute(self, r: TaskRuntime) -> None:
        gtfs_zip_path = r.resources[self.gtfs_resource].stored_at
        with self.extracted(gtfs_zip_path) as gtfs_dir:
            self.check_agencies(gtfs_dir)
            self.fix_routes(gtfs_dir)
            self.compress(gtfs_dir, gtfs_zip_path)

    @contextmanager
    def extracted(self, zip_path: Path) -> Generator[Path, None, None]:
        self.logger.debug("Extracting the GTFS")
        with TemporaryDirectory(prefix="impuls-waw") as temp_dir:
            temp_dir_path = Path(temp_dir)
            with ZipFile(zip_path, "r") as arch:
                to_extract = [i for i in arch.namelist() if "/" not in i and i.endswith(".txt")]
                arch.extractall(temp_dir_path, to_extract)
            yield temp_dir_path

    def check_agencies(self, gtfs_dir: Path) -> None:
        self.logger.debug("Checking agency.txt")
        with (gtfs_dir / "agency.txt").open("r", encoding="utf-8-sig", newline="") as f:
            valid_agencies = {i["agency_id"] for i in csv.DictReader(f)}
        if "2" not in valid_agencies:
            raise ValueError("Expected agency_id=2 (WTP) to be present in GTFS")
        if "5" not in valid_agencies:
            raise ValueError("Expected agency_id=5 (voivodeship) to be present in GTFS")

    def fix_routes(self, gtfs_dir: Path) -> None:
        self.logger.debug("Checking routes.txt")
        new_file = gtfs_dir / "routes.txt"
        old_file = new_file.rename(gtfs_dir / "routes.txt.old")

        with (
            old_file.open("r", encoding="utf-8-sig", newline="") as in_f,
            new_file.open("w", encoding="utf-8", newline="") as out_f,
        ):
            reader = csv.DictReader(in_f)
            writer = csv.DictWriter(out_f, reader.fieldnames or [])
            writer.writeheader()

            for row in reader:
                if row["agency_id"] not in {"2", "5"}:
                    self.logger.info("Fixing agency_id of route %s", row["route_id"])
                    if row["route_short_name"].startswith("R") or "WKD" in row["route_desc"]:
                        row["agency_id"] = "5"
                    else:
                        row["agency_id"] = "2"

                writer.writerow(row)

        old_file.unlink()

    def compress(self, dir: Path, zip: Path) -> None:
        self.logger.debug("Writing GTFS back")
        with ZipFile(zip, "w", compression=ZIP_DEFLATED) as arch:
            for file in dir.iterdir():
                if file.suffix != ".txt":
                    continue
                arch.write(file, file.name)
