from impuls import Task, TaskRuntime


class UpdateFeedInfo(Task):
    def __init__(self, feed_version: str) -> None:
        super().__init__()
        self.feed_version = feed_version

    def execute(self, r: TaskRuntime) -> None:
        with r.db.transaction():
            r.db.raw_execute(
                "INSERT OR REPLACE INTO feed_info "
                "(feed_info_id, publisher_name, publisher_url, lang, version) "
                "VALUES (0,'Mikołaj Kuranowski','https://mkuran.pl/gtfs/','pl',?)",
                (self.feed_version,),
            )
