from __future__ import annotations

import asyncio


from extract_products_sync import ProductExtractorSync
from extract_products_async import ProductExtractorAsync


def main() -> None:
    ProductExtractorSync().run()
    asyncio.run(ProductExtractorAsync().run())


if __name__ == "__main__":
    main()