from task_platform.api.errors import DOMAIN_ERROR_STATUS
from task_platform.domain.errors import ErrorCode


def test_every_domain_error_code_has_a_client_error_status() -> None:
    # A new ErrorCode without a status would surface as a KeyError, i.e. a 500.
    assert set(DOMAIN_ERROR_STATUS) == set(ErrorCode)
    assert all(400 <= status < 500 for status in DOMAIN_ERROR_STATUS.values())
