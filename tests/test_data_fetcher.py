from tradepy.pipelines.data_fetcher import DataFetcher, DataFetchJob


def test_data_fetch_job_ok_when_result_set_and_no_error() -> None:
    job = DataFetchJob(func=lambda: None, args={})
    assert not job.ok()

    job.result = {"rows": 1}
    assert job.ok()

    job.error_message = "failed"
    assert not job.ok()


def test_data_fetch_job_ids_are_unique() -> None:
    jobs = [DataFetchJob(func=lambda: None, args={}) for _ in range(3)]
    assert len({job.id for job in jobs}) == 3


def test_submit_with_no_jobs_yields_nothing() -> None:
    fetcher = DataFetcher(title="test")
    assert list(fetcher.submit([])) == []


def test_submit_runs_mock_fetch_and_yields_successful_jobs() -> None:
    def mock_fetch(symbol: str) -> str:
        return f"bars:{symbol}"

    jobs = [
        DataFetchJob(func=mock_fetch, args={"symbol": "000001"}),
        DataFetchJob(func=mock_fetch, args={"symbol": "000002"}),
    ]
    fetcher = DataFetcher(title="mock download")

    completed = list(fetcher.submit(jobs))

    assert len(completed) == 2
    assert {job.result for job in completed} == {"bars:000001", "bars:000002"}
    assert all(job.ok() for job in completed)
    assert all(job.error_message is None for job in completed)


def test_submit_passes_job_args_to_fetch_func() -> None:
    seen: list[dict[str, object]] = []

    def mock_fetch(**kwargs: object) -> dict[str, object]:
        seen.append(kwargs)
        return kwargs

    job = DataFetchJob(
        func=mock_fetch,
        args={"code": "600000", "start": "2020-01-01", "end": "2020-12-31"},
    )
    list(DataFetcher(title="args").submit([job]))

    assert seen == [
        {"code": "600000", "start": "2020-01-01", "end": "2020-12-31"},
    ]


def test_submit_records_error_message_when_fetch_raises() -> None:
    def mock_fetch() -> None:
        raise ValueError("quota-safe mock failure")

    job = DataFetchJob(func=mock_fetch, args={})
    list(DataFetcher(title="fail").submit([job], max_retries=0))

    assert job.result is None
    assert job.error_message is not None
    assert "quota-safe mock failure" in job.error_message
    assert not job.ok()


def test_submit_retries_failed_jobs_up_to_max_retries() -> None:
    attempts = 0

    def mock_fetch() -> str:
        nonlocal attempts
        attempts += 1
        raise OSError("transient")

    job = DataFetchJob(func=mock_fetch, args={})
    list(DataFetcher(title="retry").submit([job], max_retries=2))

    assert attempts == 3
    assert not job.ok()


def test_submit_yields_job_when_mock_fetch_succeeds_on_retry() -> None:
    attempts = 0

    def mock_fetch() -> str:
        nonlocal attempts
        attempts += 1
        if attempts < 2:
            raise RuntimeError("not ready")
        return "ok"

    job = DataFetchJob(func=mock_fetch, args={})
    completed = list(DataFetcher(title="retry-ok").submit([job], max_retries=3))

    assert len(completed) == 1
    assert completed[0].result == "ok"
    assert completed[0].ok()
    assert attempts == 2


def test_submit_yields_only_successful_jobs_when_batch_is_mixed() -> None:
    def mock_fetch(should_fail: bool) -> str:
        if should_fail:
            raise ValueError("bad symbol")
        return "ok"

    good = DataFetchJob(func=mock_fetch, args={"should_fail": False})
    bad = DataFetchJob(func=mock_fetch, args={"should_fail": True})

    completed = list(
        DataFetcher(title="mixed").submit([good, bad], max_retries=0)
    )

    assert len(completed) == 1
    assert completed[0].id == good.id
    assert completed[0].result == "ok"
    assert bad.error_message is not None
    assert not bad.ok()
