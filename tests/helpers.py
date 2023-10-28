import os
import pytest

skip_if_in_ci = lambda: pytest.mark.skipif(
    os.environ.get("CI", "false").lower() == "true",
    reason="Skip this test in CI environment",
    # Because the network access from the Github CI environment to
    # the external APIs (which are hosted in China) is unstable
)
