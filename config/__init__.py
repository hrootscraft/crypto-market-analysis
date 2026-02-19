"""Pipeline configuration.

All values sourced from environment variables with sensible defaults
for local development. Never hardcode credentials.
"""
from dataclasses import dataclass, field
from os import environ


@dataclass(frozen=True)
class Config:
    # CoinGecko API
    COINGECKO_BASE_URL: str = environ.get(
        "COINGECKO_BASE_URL", "https://api.coingecko.com/api/v3"
    )
    COINS: tuple = (
        "bitcoin", "ethereum", "solana", "cardano",
        "avalanche-2", "polkadot", "chainlink", "dogecoin",
        "ripple", "tron", "litecoin", "polygon-ecosystem-token",
        "uniswap", "stellar", "cosmos",
    )
    VS_CURRENCY: str = "usd"
    HISTORY_DAYS: int = int(environ.get("HISTORY_DAYS", "90"))

    # Postgres (data warehouse)
    POSTGRES_HOST: str = environ.get("POSTGRES_HOST", "localhost")
    POSTGRES_DB: str = environ.get("POSTGRES_DB", "crypto")
    POSTGRES_USER: str = environ.get("POSTGRES_USER", "crypto_user")
    POSTGRES_PASSWORD: str = environ.get("POSTGRES_PASSWORD", "crypto_pass")
    POSTGRES_PORT: int = int(environ.get("POSTGRES_PORT", "5433"))

    @property
    def pg_connection_string(self) -> str:
        return (
            f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}"
            f"@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )


config = Config()
