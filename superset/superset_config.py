import os

SECRET_KEY = os.environ.get(
    "SUPERSET_SECRET_KEY", "supersecret-change-me-in-production"
)

SQLALCHEMY_DATABASE_URI = "postgresql://superset:superset@superset-db:5432/superset"

# Disable example data
SUPERSET_LOAD_EXAMPLES = False

# Allow embedding dashboards in iframes (useful for sharing)
TALISMAN_ENABLED = False

# Default row limit for SQL Lab
ROW_LIMIT = 10_000
