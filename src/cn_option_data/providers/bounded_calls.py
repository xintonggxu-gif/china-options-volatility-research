"""One serial worker: hard deadlines without changing requests or AKShare globals."""

from __future__ import annotations

import contextlib
import multiprocessing as mp
import os
from pathlib import Path
import pickle
import signal
import tempfile
import time
from typing import Any, Callable
import uuid

from ..models import now_shanghai


class NetworkCallError(RuntimeError):
    pass


class CallCancelled(RuntimeError):
    pass


class RequestBudgetExceeded(RuntimeError):
    pass


def _worker(connection: Any, directory: str, http_counter: Any = None, max_http_requests: int | None = None) -> None:
    # Terminal Ctrl+C goes to the collector. Parent terminates this process explicitly.
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
    import requests

    if http_counter is not None:
        original_request = requests.sessions.Session.request

        def counted_request(session: Any, method: str, url: str, **kwargs: Any) -> Any:
            with http_counter.get_lock():
                if http_counter.value >= max_http_requests:
                    raise RequestBudgetExceeded("HTTP request budget exhausted")
                http_counter.value += 1
            return original_request(session, method, url, **kwargs)

        requests.sessions.Session.request = counted_request

    session = requests.Session()
    ak = None
    with open(os.devnull, "w") as quiet, contextlib.redirect_stdout(quiet), contextlib.redirect_stderr(quiet):
        while True:
            try:
                task = connection.recv()
            except EOFError:
                break
            if task is None:
                break
            token, kind, name, kwargs = task
            requested = None
            try:
                if kind == "akshare" and ak is None:
                    import akshare as ak
                requested = now_shanghai()
                if kind == "akshare":
                    data = getattr(ak, name)(**kwargs)
                elif kind == "http":
                    with session.get(name, timeout=(3, 10), **kwargs) as response:
                        response.raise_for_status()
                        data = response.content
                else:
                    raise ValueError("unknown operation")
                result = (True, data, requested, now_shanghai())
            except Exception as exc:
                # Raw exception text can contain proxy credentials. Keep only safe categories.
                status = getattr(getattr(exc, "response", None), "status_code", None)
                result = (False, f"{type(exc).__name__}; http_status={status}", requested, now_shanghai())
            target = Path(directory) / f"{token}.pkl"
            temporary = target.with_suffix(".tmp")
            with temporary.open("wb") as handle:
                pickle.dump(result, handle, protocol=pickle.HIGHEST_PROTOCOL)
            os.replace(temporary, target)
    session.close()
    connection.close()


class BoundedCalls:
    """Reuse the worker (and AKShare's own cache), replacing it only after failure/stop.

    Result files are private, locally produced IPC, not provider pickle data. Atomic
    completion avoids blocking on a partially written large multiprocessing pipe.
    Deadlines include import/network/serialization; termination adds at most 2 seconds.
    """

    def __init__(self, *, timeout: float = 15.0, should_stop: Callable[[], bool] = lambda: False,
                 on_wait: Callable[[], None] = lambda: None, max_http_requests: int | None = None):
        if max_http_requests is not None and max_http_requests <= 0:
            raise ValueError("max_http_requests must be positive")
        self.timeout = timeout
        self.should_stop = should_stop
        self.on_wait = on_wait
        self.max_http_requests = max_http_requests
        self._context = mp.get_context("spawn")
        self._http_counter = self._context.Value("i", 0) if max_http_requests is not None else None
        self._directory = tempfile.TemporaryDirectory(prefix="cn-option-http-")
        self._process: Any = None
        self._connection: Any = None

    def _start(self) -> None:
        if self._process is not None and self._process.is_alive():
            return
        self._terminate()
        parent, child = self._context.Pipe()
        process = self._context.Process(
            target=_worker,
            args=(child, self._directory.name, self._http_counter, self.max_http_requests),
            daemon=True,
        )
        try:
            process.start()
        except BaseException:
            parent.close()
            child.close()
            raise
        child.close()
        self._connection, self._process = parent, process

    def _terminate(self) -> None:
        if self._process is not None:
            if self._process.is_alive():
                self._process.terminate()
                self._process.join(1)
            if self._process.is_alive():
                self._process.kill()
                self._process.join(1)
            if self._process.is_alive():
                raise RuntimeError("请求进程未终止，拒绝创建重叠请求")
            self._process.join()
            self._process.close()
            self._process = None
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    def call(self, kind: str, name: str, **kwargs: Any) -> tuple[Any, Any, Any]:
        if self.should_stop():
            raise CallCancelled("停止发起新请求")
        started = time.monotonic()
        token = uuid.uuid4().hex
        target = Path(self._directory.name) / f"{token}.pkl"
        def completed_result() -> tuple[Any, Any, Any]:
            with target.open("rb") as handle:
                ok, data, requested, received = pickle.load(handle)
            if not ok:
                raise NetworkCallError(f"{name}: {data}; requested_at={requested}; received_at={received}")
            return data, requested, received
        try:
            self._start()
            if self.should_stop():
                raise CallCancelled("停止发起新请求")
            self._connection.send((token, kind, name, kwargs))
            while True:
                # Consume completed results even if stop was requested at the same time.
                if target.exists():
                    return completed_result()
                if self.should_stop():
                    self._terminate()
                    if target.exists():
                        return completed_result()
                    raise CallCancelled("请求未完成，已终止请求进程")
                if time.monotonic() - started >= self.timeout:
                    self._terminate()
                    if target.exists():
                        return completed_result()
                    raise NetworkCallError(f"{name}: HARD_TIMEOUT ({self.timeout:g}s)，请求进程已结束")
                if not self._process.is_alive():
                    raise NetworkCallError(f"{name}: WORKER_EXIT")
                self.on_wait()
                time.sleep(0.05)
        except NetworkCallError:
            raise
        except BaseException:
            self._terminate()
            raise
        finally:
            target.unlink(missing_ok=True)
            target.with_suffix(".tmp").unlink(missing_ok=True)

    def close(self) -> None:
        self._terminate()
        self._directory.cleanup()

    @property
    def http_requests(self) -> int:
        return self._http_counter.value if self._http_counter is not None else 0
