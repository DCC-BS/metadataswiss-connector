"""OAuth2 client-credentials authentication for the I14Y Partner API."""

import time

import httpx


class I14YAuth(httpx.Auth):
    """Authenticate against I14Y via Keycloak client_credentials flow.

    Automatically fetches and refreshes the bearer token before it expires.

    Usage:

        auth = I14YAuth(
            token_url="https://identity.i14y.a.c.bfs.admin.ch/realms/bfs-sis-a/protocol/openid-connect/token",
            client_id="my-client",
            client_secret="secret",
        )
        client = httpx.Client(auth=auth)
        resp = client.get("https://api-a.i14y.admin.ch/api/partner/v1/datasets")
    """

    def __init__(
        self,
        token_url: str,
        client_id: str,
        client_secret: str,
        *,
        refresh_margin_seconds: int = 30,
    ) -> None:
        self.token_url = token_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_margin_seconds = refresh_margin_seconds

        self._access_token: str | None = None
        self._expires_at: float = 0.0

    def auth_flow(self, request: httpx.Request):
        if self._is_expired():
            self._fetch_token()
        request.headers["Authorization"] = f"Bearer {self._access_token}"
        yield request

    def _is_expired(self) -> bool:
        return (
            self._access_token is None
            or time.monotonic() >= self._expires_at - self.refresh_margin_seconds
        )

    def _fetch_token(self) -> None:
        response = httpx.post(
            self.token_url,
            data={
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            },
        )
        response.raise_for_status()
        data = response.json()
        self._access_token = data["access_token"]
        self._expires_at = time.monotonic() + data.get("expires_in", 300)
