import os
import pytest


# Tests marked as perf are skipped by default and can be enabled with
# the flag --live when calling pytest
def pytest_addoption(parser):
    parser.addoption(
        "--perf",
        action="store_true",
        dest="perf",
        default=False,
        help="enable only perf tests",
    )


PYTEST_ENV_VAR_OVERRIDES = {
    "AWS_ACCESS_KEY_ID": "testing",
    "AWS_SECRET_ACCESS_KEY": "testing",
    "AWS_SECURITY_TOKEN": "testing",
    "AWS_SESSION_TOKEN": "testing",
}


def stash_env_var_overrides(stash):
    original_env = {
        envVar: os.environ.get(envVar, None) for envVar in PYTEST_ENV_VAR_OVERRIDES
    }

    stash.setdefault("overridden_env", original_env)


def override_env_vars():
    for envVar, value in PYTEST_ENV_VAR_OVERRIDES.items():
        os.environ[envVar] = value


def restore_env_vars_from_stash(stash):
    original_env = stash.get("overridden_env", {})

    for envVar, value in original_env.items():
        if value is None:
            del os.environ[envVar]
        else:
            os.environ[envVar] = value


def pytest_configure(config):
    stash_env_var_overrides(config.stash)
    override_env_vars()

    def append_to_markexpr(what):
        if hasattr(config.option, "markexpr") and getattr(config.option, "markexpr"):
            what = f"{getattr(config.option, 'markexpr')} and {what}"
        setattr(config.option, "markexpr", what)

    if config.option.perf:
        append_to_markexpr("perf")
    else:
        append_to_markexpr("not perf")


@pytest.fixture(scope="function")
def native_env(pytestconfig):
    """Restore Environment Variables for tests that need external resources (e.g., S3)."""
    restore_env_vars_from_stash(pytestconfig.stash)

    yield

    override_env_vars()
