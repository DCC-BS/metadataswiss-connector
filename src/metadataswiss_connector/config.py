"""Framework-free configuration objects.

These live outside of the orchestrator so the connector can be used
as a plain library (CLI, tests, notebooks) without pulling in Airflow.
"""

import os

from pydantic import BaseModel


class I14YConfig(BaseModel):
    """Connection + publisher config for the I14Y Partner API."""

    base_url: str
    token_url: str
    client_id: str
    client_secret: str
    publisher: str
    user_agent: str

    @classmethod
    def from_env(cls) -> "I14YConfig":
        return cls(
            base_url=os.getenv(
                "I14Y_BASE_URL", "https://api-a.i14y.admin.ch/api/partner/v1"
            ),
            token_url=os.getenv(
                "I14Y_TOKEN_URL",
                "https://identity.i14y.a.c.bfs.admin.ch/realms/bfs-sis-a/protocol/openid-connect/token",
            ),
            client_id=os.getenv("I14Y_CLIENT_ID", ""),
            client_secret=os.getenv("I14Y_CLIENT_SECRET", ""),
            publisher=os.getenv("I14Y_PUBLISHER_IDENTIFIER", ""),
            user_agent=os.getenv("I14Y_USER_AGENT", ""),
        )
