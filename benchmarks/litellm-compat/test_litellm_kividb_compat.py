"""Offline regression checks for the harness's chat model selection."""
import fnmatch
import socket
import sys
from unittest.mock import Mock

import litellm_kividb_compat as harness
import pytest


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    def blocked(*args, **kwargs):
        raise AssertionError("network access is disabled")

    monkeypatch.setattr(socket.socket, "connect", blocked)
    monkeypatch.setattr(socket.socket, "connect_ex", blocked)
    monkeypatch.setenv("LITELLM_LOCAL_MODEL_COST_MAP", "True")
    for name in ("OPENAI_API_KEY", "OPENAI_API_BASE", "OPENAI_BASE_URL"):
        monkeypatch.setenv(name, "")


@pytest.mark.parametrize("args, model", [
    ([], "openai/gpt-6-luna"),
    (["--model", "openai/custom-chat-model"], "openai/custom-chat-model"),
])
def test_cli_model(monkeypatch, tmp_path, args, model):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "argv", ["harness", "--url", "redis://localhost:6379", *args])
    client = Mock()
    client.scan_iter.return_value = []
    monkeypatch.setattr(harness.redis.Redis, "from_url", lambda url: client)
    monkeypatch.setattr(harness, "primitives", Mock())
    monkeypatch.setattr(harness, "raw_vector", Mock())
    monkeypatch.setattr(harness, "start_stub", lambda: 12345)
    checks = Mock()
    monkeypatch.setattr(harness, "litellm_checks", checks)
    monkeypatch.setattr(harness, "results", [])

    with pytest.raises(SystemExit) as exit_info:
        harness.main()

    assert exit_info.value.code == 0
    assert checks.call_args.args[2:] == (12345, model)


def test_cache_requests_use_selected_model(monkeypatch):
    import litellm
    from litellm.caching import caching

    class RequestCaptured(Exception):
        pass

    completion = Mock(side_effect=RequestCaptured)
    monkeypatch.setattr(litellm, "completion", completion)
    monkeypatch.setattr(litellm, "cache", None)
    monkeypatch.setattr(litellm, "drop_params", False)
    monkeypatch.setattr(caching, "Cache", Mock())
    monkeypatch.setattr(harness.redis.Redis, "from_url", Mock())

    def check(name, fn, level="FAIL"):
        if "cache" in name:
            with pytest.raises(RequestCaptured):
                fn()

    monkeypatch.setattr(harness, "check", check)
    harness.litellm_checks("redis://localhost:6379", "test:", 12345, "openai/custom-chat-model")

    assert completion.call_count == 2
    assert all(call.kwargs["model"] == "openai/custom-chat-model" for call in completion.call_args_list)


@pytest.mark.parametrize("model", ["openai/gpt-6-luna", "openai/custom-chat-model"])
def test_router_counters_follow_model(monkeypatch, model):
    import litellm

    key = f"deployment:{model}:rpm:12-00".encode()
    values = {key: b"7"}
    client = Mock()
    client.scan_iter.side_effect = lambda match, **kw: fnmatch.filter(values, match.encode())
    client.mget.side_effect = lambda keys: [values[k] for k in keys]
    monkeypatch.setattr(harness.redis.Redis, "from_url", lambda url: client)
    monkeypatch.setattr(harness.time, "sleep", lambda seconds: None)
    monkeypatch.setattr(litellm, "cache", None)
    monkeypatch.setattr(litellm, "drop_params", False)

    def complete(**kwargs):
        values[key] = str(int(values[key]) + 1).encode()

    router = Mock()
    router.return_value.completion.side_effect = complete
    monkeypatch.setattr(litellm, "Router", router)
    details = []

    def check(name, fn, level="FAIL"):
        if "router" in name:
            details.append(fn())

    monkeypatch.setattr(harness, "check", check)
    harness.litellm_checks("redis://localhost:6379", "test:", 12345, model)

    assert details == ["1 counter keys grew, rpm +3"]
    assert router.call_args.kwargs["model_list"][0]["litellm_params"]["model"] == model
