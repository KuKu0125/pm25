import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def build_session(
    total_retries: int = 5,
    backoff_factor: float = 0.5,
    status_forcelist = (429, 500, 502, 503, 504),
    allowed_methods = ("GET", "POST"),
    timeout: int = 30,
):
    session = requests.Session()

    retry = Retry(
        total=total_retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=frozenset(allowed_methods),
        raise_on_status=False,
        respect_retry_after_header=True,
    )

    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    # 封裝帶預設 timeout 的 get/post
    def _get(url, **kwargs):
        kwargs.setdefault("timeout", timeout)
        return session.get(url, **kwargs)

    def _post(url, **kwargs):
        kwargs.setdefault("timeout", timeout)
        return session.post(url, **kwargs)

    session.get_with_timeout = _get
    session.post_with_timeout = _post
    return session