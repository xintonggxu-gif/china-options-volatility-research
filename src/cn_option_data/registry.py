"""The CSV registry is the sole source of option product scope."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Product:
    product_id: str
    exchange: str
    option_category: str
    underlying_symbol: str
    hedge_future: str | None


def load_registry(path: Path, selection: str = "all") -> list[Product]:
    if not path.is_file():
        raise FileNotFoundError(f"产品注册表不存在: {path}")
    products: list[Product] = []
    with path.open(newline="", encoding="utf-8-sig") as handle:
        for row in csv.DictReader(handle):
            if row["status"].strip().upper() != "ACTIVE":
                continue
            products.append(
                Product(
                    product_id=row["product_id"].strip(),
                    exchange=row["exchange"].strip().upper(),
                    option_category=row["option_category"].strip().upper(),
                    underlying_symbol=row["provider_underlying_symbol"].strip(),
                    hedge_future=row["hedge_future"].strip().upper() or None,
                )
            )
    if not products or len({p.product_id for p in products}) != len(products):
        raise ValueError("注册表为空或 product_id 重复")
    for product in products:
        if product.exchange not in {"SSE", "SZSE", "CFFEX"}:
            raise ValueError(f"未知交易所: {product.product_id}")
        if product.option_category not in {"ETF", "INDEX"}:
            raise ValueError(f"未知期权类型: {product.product_id}")
        if not product.underlying_symbol or "." not in product.underlying_symbol:
            raise ValueError(f"缺少准确的标的代码: {product.product_id}")
        if product.option_category == "ETF" and product.underlying_symbol != f"{product.exchange}.{product.product_id}":
            raise ValueError(f"ETF 标的代码与产品不一致: {product.product_id}")
        if product.option_category == "INDEX" and product.exchange != "CFFEX":
            raise ValueError(f"指数期权交易所错误: {product.product_id}")
    if selection == "all":
        return products
    wanted = {value.strip() for value in selection.split(",") if value.strip()}
    unknown = wanted - {p.product_id for p in products}
    if not wanted or unknown:
        raise ValueError(f"未注册的产品: {', '.join(sorted(unknown or wanted))}")
    return [p for p in products if p.product_id in wanted]
