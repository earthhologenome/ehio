"""Config loading compatibility tests."""

from ehio import config


def test_legacy_airtable_config_keys_are_ignored(tmp_path, monkeypatch):
    path = tmp_path / "config.yaml"
    path.write_text('EHI_BASE: old-value\nEHI_GENOME_CODE: old-field\nEHI_CORE_URL: https://core.test\n')
    monkeypatch.setattr(config, "CONFIG_PATH", path)
    assert config.get("EHI_CORE_URL") == "https://core.test"
