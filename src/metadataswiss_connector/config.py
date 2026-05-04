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
            base_url=os.environ["I14Y_BASE_URL"],
            token_url=os.environ["I14Y_TOKEN_URL"],
            client_id=os.environ["I14Y_CLIENT_ID"],
            client_secret=os.environ["I14Y_CLIENT_SECRET"],
            publisher=os.environ["I14Y_PUBLISHER_IDENTIFIER"],
            user_agent=os.environ["I14Y_USER_AGENT"],
        )
