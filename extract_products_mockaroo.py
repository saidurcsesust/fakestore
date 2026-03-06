from __future__ import annotations

import asyncio


from extract_products_mockaroo_sync import ProductExtractorMockarooSync
from extract_products_mockaroo_async import ProductExtractorMockarooAsync


def main() -> None:
    ProductExtractorMockarooSync().run()
    asyncio.run(ProductExtractorMockarooAsync().run())


if __name__ == "__main__":
    main()