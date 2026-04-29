import importlib.util
from pathlib import Path


CHECK_SECRETS_PATH = Path(__file__).resolve().parents[1] / "scripts" / "check_secrets.py"
spec = importlib.util.spec_from_file_location("check_secrets", CHECK_SECRETS_PATH)
check_secrets = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(check_secrets)


def test_secret_check_flags_env_key_with_literal_secret(tmp_path: Path) -> None:
    config = tmp_path / "bad.yaml"
    config.write_text(
        'client_id_env: "OMF_CLIENT_ID"\n'
        'client_secret_env: "not-an-env-var-value-with-symbols/and=suffix"\n',
        encoding="utf-8",
    )

    issues = check_secrets._find_issues(config)

    assert issues == [(2, "client_secret_env is not an env var name")]


def test_secret_check_allows_env_var_names(tmp_path: Path) -> None:
    config = tmp_path / "good.yaml"
    config.write_text(
        'client_id_env: "OMF_CLIENT_ID"\n'
        'client_secret_env: "OMF_CLIENT_SECRET"\n',
        encoding="utf-8",
    )

    assert check_secrets._find_issues(config) == []
