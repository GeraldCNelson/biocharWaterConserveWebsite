import json

from biochar_app.pakbus.core.gateway_discovery import discover_address, update_settings_host


def test_discover_address_selects_global_campbell_suffix(monkeypatch):
    import biochar_app.pakbus.core.gateway_discovery as discovery

    responses = iter([
        '{"asus_token":"ok"}',
        '00:D0:2C:02:1D:DD 2605:59ca:227f:e00:2d0:2cff:fe02:1ddd fe80::2d0:2cff:fe02:1ddd',
        '',
    ])
    monkeypatch.setattr(discovery, "_opener", lambda _verify: object())
    monkeypatch.setattr(discovery, "_read", lambda *_args, **_kwargs: next(responses))
    assert discover_address(
        "https://router", "user", "password", "00:D0:2C:02:1D:DD", "2d0:2cff:fe02:1ddd", False
    ) == (
        "2605:59ca:227f:e00:2d0:2cff:fe02:1ddd"
    )


def test_update_settings_host_changes_only_connection_host(tmp_path):
    path = tmp_path / "settings.json"
    original = {"connection": {"host": "old", "port": 6785}, "daily": {"hours": 24}}
    path.write_text(json.dumps(original), encoding="utf-8")
    update_settings_host(path, "2605:59ca:227f:e00:2d0:2cff:fe02:1ddd")
    updated = json.loads(path.read_text(encoding="utf-8"))
    assert updated["connection"]["host"] == "2605:59ca:227f:e00:2d0:2cff:fe02:1ddd"
    assert updated["connection"]["port"] == 6785
    assert updated["daily"] == original["daily"]
