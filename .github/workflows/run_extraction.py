"""
Thin CLI wrapper so GitHub Actions (or any scheduler) can run the
`perform_osp_extraction()` coroutine once and exit with an appropriate code.
"""

import asyncio
import logging
import sys

# Import the main Quart application module.
# If your file isn’t called app.py, change the import accordingly.
import app as osp_app

logging.getLogger().setLevel(logging.INFO)


async def main() -> None:
    """Run the extraction and exit with 0 on success, 1 on failure."""
    ok = await osp_app.perform_osp_extraction()
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    asyncio.run(main())
