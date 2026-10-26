import pytest

from bullet_trade.remote.connection import RemoteQmtConnection


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["broker.place_order", "broker.cancel_order"])
async def test_trade_disconnect_is_not_automatically_replayed(action):
    conn = RemoteQmtConnection("unused", 1, "test")
    conn._connected.set()
    sent = []

    async def send(message):
        sent.append(message)
        if len(sent) == 1:
            conn._pending[message["id"]].set_exception(RuntimeError("连接已断开"))
        else:
            conn._pending[message["id"]].set_result({"unexpected_replay": True})

    conn._send = send
    with pytest.raises(RuntimeError, match="submit_unknown"):
        await conn._request_async(action, {})
    assert len(sent) == 1
    if action == "broker.place_order":
        assert sent[0]["payload"]["idempotency_key"]


@pytest.mark.asyncio
async def test_read_queries_still_retry_after_disconnect():
    conn = RemoteQmtConnection("unused", 1, "test")
    conn._connected.set()
    sent = []

    async def send(message):
        sent.append(message)
        future = conn._pending[message["id"]]
        if len(sent) == 1:
            future.set_exception(RuntimeError("连接已断开"))
        else:
            future.set_result({"positions": []})

    conn._send = send
    assert await conn._request_async("broker.positions", {}) == {"positions": []}
    assert len(sent) == 2
