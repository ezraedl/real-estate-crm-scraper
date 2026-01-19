"""
Rentcast rent estimation service.

Scrapes app.rentcast.io (unauthenticated) for rent, rent_range_low, rent_range_high,
and comparables. Uses the same proxy rotation and browser-headers concept as Realtor.
The app is Angular and loads data via JS, so we use Playwright.

Requires: playwright install chromium (run once per environment).
"""

import asyncio
import json
import logging
import re
import secrets
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Union
from urllib.parse import urlencode

from proxy_manager import proxy_manager

logger = logging.getLogger(__name__)

# Rate limiting: max 2 concurrent Rentcast requests, 1–2s between requests
_RENTCAST_SEMAPHORE = asyncio.Semaphore(2)


def _playwright_proxy_dict(proxy) -> Optional[Dict[str, str]]:
    """Build Playwright context proxy dict from ProxyConfig."""
    if not proxy:
        return None
    d: Dict[str, str] = {"server": f"http://{proxy.host}:{proxy.port}"}
    username = proxy.username
    if username and "session." not in username:
        username = f"{username};session.{secrets.randbelow(1_000_000)}"
    if username and proxy.password:
        d["username"] = username
        d["password"] = proxy.password
    return d


def _build_address(property_dict: dict) -> Optional[str]:
    """Build address string from property_dict. Prefer formatted_address."""
    addr = property_dict.get("address")
    if not isinstance(addr, dict):
        return None
    fa = addr.get("formatted_address")
    if fa and str(fa).strip():
        return str(fa).strip()
    street = (addr.get("street") or addr.get("full_street_line") or "").strip()
    unit = (addr.get("unit") or "").strip()
    city = (addr.get("city") or "").strip()
    state = (addr.get("state") or "").strip()
    zip_code = (addr.get("zip_code") or "").strip()
    parts = [f"{street} {unit}".strip(), city, f"{state} {zip_code}".strip()]
    built = ", ".join(p for p in parts if p)
    return built if built else None


def _build_rentcast_url(property_dict: dict) -> Optional[str]:
    """Build app.rentcast.io URL with address and optional type, bedrooms, bathrooms, area, year."""
    address = _build_address(property_dict)
    if not address:
        return None
    params: Dict[str, Any] = {"address": address}
    desc = property_dict.get("description")
    if isinstance(desc, dict):
        pt = desc.get("property_type") or desc.get("style")
        if pt:
            params["type"] = str(pt).lower().replace(" ", "-")
        if desc.get("beds") is not None:
            params["bedrooms"] = int(desc["beds"])
        fb = desc.get("full_baths")
        hb = (desc.get("half_baths") or 0) * 0.5
        if fb is not None or hb:
            params["bathrooms"] = (fb or 0) + hb
        if desc.get("sqft") is not None:
            params["area"] = int(desc["sqft"])
        if desc.get("year_built") is not None:
            params["year"] = int(desc["year_built"])
    base = "https://app.rentcast.io/app"
    return f"{base}?{urlencode(params)}"


# Plausible monthly rent range: exclude per-sqft ($0.94, $1.04), per-bedroom ($563), etc.
_RENT_MIN, _RENT_MAX = 600, 50_000


def _num(x: Any) -> Optional[Union[int, float]]:
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return int(x) if isinstance(x, float) and x == int(x) else x
    try:
        return int(x)
    except (TypeError, ValueError):
        try:
            return float(x)
        except (TypeError, ValueError):
            return None


def _find_rentcast_data(obj: Any, depth: int = 0) -> Optional[Dict[str, Any]]:
    """Recursively find a dict that has rent and (rentRangeLow or rent_range_low or comparables)."""
    if depth > 14:
        return None
    if isinstance(obj, dict):
        rent = obj.get("rent") or obj.get("rentAmount") or obj.get("amount")
        low = obj.get("rentRangeLow") or obj.get("rent_range_low") or obj.get("low")
        high = obj.get("rentRangeHigh") or obj.get("rent_range_high") or obj.get("high")
        comps = obj.get("comparables") or obj.get("comps")
        if comps is None and "comp" in obj and isinstance(obj["comp"], list):
            comps = obj["comp"]
        if rent is not None and isinstance(rent, (int, float)) and (
            low is not None or high is not None or (isinstance(comps, list) and len(comps) > 0)
        ):
            return obj
        for v in obj.values():
            found = _find_rentcast_data(v, depth + 1)
            if found:
                return found
    elif isinstance(obj, list):
        for i in obj:
            found = _find_rentcast_data(i, depth + 1)
            if found:
                return found
    return None


def _extract_rent_estimation_from_obj(obj: Dict[str, Any]) -> Dict[str, Any]:
    """Build rent_estimation from a Rentcast-like JSON object."""
    rent = _num(obj.get("rent") or obj.get("rentAmount") or obj.get("amount"))
    low = _num(obj.get("rentRangeLow") or obj.get("rent_range_low") or obj.get("low"))
    high = _num(obj.get("rentRangeHigh") or obj.get("rent_range_high") or obj.get("high"))
    comps = obj.get("comparables") or obj.get("comps") or []
    if not isinstance(comps, list):
        comps = []
    # Keep comp objects as-is; they may have formattedAddress, rent, bedrooms, etc.
    comps = [c if isinstance(c, dict) else {"rent": _num(c)} for c in comps[:25]]
    subject = obj.get("subjectProperty") or obj.get("subject_property")
    if subject is not None and not isinstance(subject, dict):
        subject = None
    return {
        "rent": rent,
        "rent_range_low": low,
        "rent_range_high": high,
        "comparables": comps,
        "subject_property": subject,
    }


def _extract_from_getrentdata_response(obj: dict) -> Optional[Dict[str, Any]]:
    """
    Extract from /getRentData API response: data.property.rentEstimate.{value,rangeMin,rangeMax}
    and data.comps[]. Normalizes comps to {id, formattedAddress, rent, bedrooms, bathrooms,
    squareFootage, distance, daysOld, correlation}.
    """
    try:
        d = obj.get("data")
        if not isinstance(d, dict):
            return None
        prop = d.get("property")
        if not isinstance(prop, dict):
            return None
        re_ = prop.get("rentEstimate")
        if not isinstance(re_, dict):
            return None
        rent = _num(re_.get("value"))
        if rent is None:
            return None
        low = _num(re_.get("rangeMin"))
        high = _num(re_.get("rangeMax"))
        subject = {"address": prop.get("address"), "description": prop.get("description")}
        comps: List[Dict[str, Any]] = []
        for c in (d.get("comps") or [])[:25]:
            if not isinstance(c, dict):
                continue
            ad = c.get("address") or {}
            parts = [ad.get("street"), ad.get("city"), ad.get("state"), ad.get("zip")]
            formatted = ", ".join(str(p) for p in parts if p)
            lst = c.get("listing") or {}
            desc = c.get("description") or {}
            comps.append({
                "id": c.get("id"),
                "formattedAddress": formatted or c.get("id"),
                "rent": _num(lst.get("price")),
                "bedrooms": _num(desc.get("bedrooms")),
                "bathrooms": _num(desc.get("bathrooms")),
                "squareFootage": _num(desc.get("livingAreaSize")),
                "distance": _num(c.get("distance")),
                "daysOld": _num(lst.get("daysOld")),
                "correlation": _num(c.get("correlation")),
            })
        return {
            "rent": rent,
            "rent_range_low": low,
            "rent_range_high": high,
            "comparables": comps,
            "subject_property": subject,
        }
    except Exception:
        return None


def _parse_rent_from_text(text: str) -> Dict[str, Any]:
    """
    Extract rent, rent_range_low, rent_range_high from page text.

    Rentcast shows: Estimated Monthly Rent; Low Estimate; High Estimate. The page
    also has per-sqft and per-bedroom amounts; we filter to plausible monthly
    rents (600–50k) and pick the tightest triple (low/estimate/high) via the
    three consecutive values with smallest range.
    """
    res: Dict[str, Any] = {
        "rent": None,
        "rent_range_low": None,
        "rent_range_high": None,
        "comparables": [],
    }
    dollar = re.findall(r"\$[\s]?([0-9,]+)(?:\.[0-9]{2})?", text)
    if not dollar:
        return res
    values: List[int] = [int(v.replace(",", "")) for v in dollar]
    # Keep only plausible monthly rents (exclude $0, $1, $0.94, $1.04, $563/bed, etc.)
    rent_like = [v for v in values if _RENT_MIN <= v <= _RENT_MAX]

    if len(rent_like) >= 3:
        s = sorted(rent_like)
        # Pick the 3 consecutive values with smallest range (the main low/estimate/high)
        best = (s[0], s[1], s[2])
        best_range = s[2] - s[0]
        for i in range(1, len(s) - 2):
            a, b, c = s[i], s[i + 1], s[i + 2]
            r = c - a
            if r < best_range:
                best_range = r
                best = (a, b, c)
        res["rent_range_low"], res["rent"], res["rent_range_high"] = best[0], best[1], best[2]
    elif len(rent_like) == 2:
        a, b = sorted(rent_like)
        res["rent_range_low"], res["rent_range_high"] = a, b
        res["rent"] = (a + b) // 2
    elif len(rent_like) == 1:
        res["rent"] = rent_like[0]
    else:
        # Fallback: use first $ amount as rent (may include noise)
        res["rent"] = values[0]
        ok = [v for v in values[:10] if _RENT_MIN <= v <= _RENT_MAX]
        if len(ok) >= 2:
            res["rent_range_low"], res["rent_range_high"] = min(ok), max(ok)
    return res


async def _parse_comps_from_page(page) -> List[Dict[str, Any]]:
    """
    Try to extract comparables from the Rentcast page DOM. Comps are often in a
    table or list below the main estimate. Returns [] if not found or on error.
    """
    comps: List[Dict[str, Any]] = []
    try:
        # Try table rows first (common for comps)
        rows = await page.query_selector_all("table tbody tr")
        for row in rows[:15]:
            text = (await row.inner_text()).strip()
            if not text:
                continue
            # Rent: first $ amount in plausible monthly range
            m = re.search(r"\$[\s]?([0-9,]+)(?:\.[0-9]{2})?", text)
            rent = None
            if m:
                v = int(m.group(1).replace(",", ""))
                if _RENT_MIN <= v <= _RENT_MAX:
                    rent = v
            # Address: look for "City, ST" or "..., ST 12345" or "123 X St"
            addr = None
            if re.search(r",\s*[A-Za-z]{2}\s*,?\s*\d{5}", text):
                # "City, ST 12345" or "City, ST, 12345"
                addr = re.sub(r"\s+", " ", text.split("\n")[0] if "\n" in text else text)[:120]
            elif re.search(r"\d+\s+[\w\s]+(?:St|Street|Ave|Avenue|Dr|Lane|Ln|Blvd|Rd)\b", text, re.I):
                addr = re.sub(r"\s+", " ", text)[:120]
            if rent is not None:
                comps.append({"formattedAddress": addr or text[:100], "rent": rent})
        # If no table, try elements with "comparab" or "comp" or "recent" in class
        if not comps:
            nodes = await page.query_selector_all(
                '[class*="comparab"], [class*="Comparab"], [class*="comp"], [class*="recent"]'
            )
            for el in nodes[:3]:
                sub = await el.query_selector_all("tr, [class*='row'], [class*='item']")
                for s in sub[:10]:
                    text = (await s.inner_text()).strip()
                    m = re.search(r"\$[\s]?([0-9,]+)(?:\.[0-9]{2})?", text)
                    if m:
                        v = int(m.group(1).replace(",", ""))
                        if _RENT_MIN <= v <= _RENT_MAX:
                            comps.append({"formattedAddress": text[:100], "rent": v})
    except Exception:
        pass
    return comps[:10]


class RentcastService:
    """Fetches rent estimates from app.rentcast.io (unauthenticated) and saves to property."""

    def __init__(self, db=None):
        self.db = db

    async def _fetch_and_parse(self, property_dict: dict, jitter_key: str = "") -> Optional[Dict[str, Any]]:
        """Fetch Rentcast page and parse rent. Returns rent_estimation dict or None. No DB."""
        url = _build_rentcast_url(property_dict)
        if not url:
            return None
        proxy = proxy_manager.get_next_proxy()
        proxy_dict = _playwright_proxy_dict(proxy)
        async with _RENTCAST_SEMAPHORE:
            await asyncio.sleep(1.0 + (hash(jitter_key) % 1000) / 1000.0)
        try:
            from playwright.async_api import async_playwright

            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                try:
                    ctx_opts: Dict[str, Any] = {"ignore_https_errors": True}
                    if proxy_dict:
                        ctx_opts["proxy"] = proxy_dict
                    context = await browser.new_context(**ctx_opts)
                    page = await context.new_page()
                    api_responses: List[Any] = []

                    def collect_response(resp: Any) -> None:
                        try:
                            if resp.request.resource_type in ("xhr", "fetch"):
                                api_responses.append(resp)
                        except Exception:
                            pass

                    page.on("response", collect_response)
                    await page.goto(url, wait_until="domcontentloaded", timeout=20000)
                    try:
                        await page.get_by_text(re.compile(r"\$[\d,]+")).first.wait_for(state="visible", timeout=20000)
                    except Exception:
                        pass
                    await asyncio.sleep(2)

                    # 1) JSON from XHR/fetch: prefer /getRentData (data.property.rentEstimate, data.comps)
                    data = None
                    ordered = [r for r in api_responses if "getRentData" in (r.url or "")] + [
                        r for r in api_responses if "getRentData" not in (r.url or "")
                    ]
                    for r in ordered:
                        try:
                            raw = await r.body()
                            obj = json.loads(raw.decode("utf-8", errors="ignore"))
                            data = _extract_from_getrentdata_response(obj)
                            if data is None:
                                found = _find_rentcast_data(obj)
                                if found:
                                    data = _extract_rent_estimation_from_obj(found)
                            if data is not None:
                                break
                        except Exception:
                            pass

                    # 2) JSON from <script type="application/json"> or #__NEXT_DATA__
                    if data is None:
                        for sel in ('script[type="application/json"]', "script#__NEXT_DATA__"):
                            els = await page.query_selector_all(sel)
                            for el in els:
                                try:
                                    text = await el.inner_text()
                                    if not text or len(text) < 50:
                                        continue
                                    obj = json.loads(text)
                                    found = _find_rentcast_data(obj)
                                    if found:
                                        data = _extract_rent_estimation_from_obj(found)
                                        break
                                except Exception:
                                    pass
                            if data is not None:
                                break

                    # 3) Fallback: parse rendered text and DOM comps
                    if data is None:
                        text = await page.inner_text("body")
                        data = _parse_rent_from_text(text)
                        data["comparables"] = await _parse_comps_from_page(page)
                    elif not data.get("comparables"):
                        data["comparables"] = await _parse_comps_from_page(page)
                finally:
                    try:
                        await browser.close()
                    except Exception:
                        pass
        except ImportError as e:
            logger.warning(
                "Rentcast: playwright not installed: %s. "
                "Run: pip install playwright && playwright install chromium",
                e,
            )
            return None
        except Exception as e:
            logger.warning("Rentcast: fetch/parse error: %s", e)
            return None
        if data.get("rent") is None:
            return None
        return {
            "rent": data["rent"],
            "rent_range_low": data.get("rent_range_low"),
            "rent_range_high": data.get("rent_range_high"),
            "comparables": data.get("comparables") or [],
            "subject_property": data.get("subject_property"),
            "fetched_at": datetime.utcnow(),
            "source": "rentcast",
        }

    async def fetch_rent_estimate(self, property_dict: dict) -> Optional[Dict[str, Any]]:
        """
        Fetch rent estimate from Rentcast app only (no DB save).
        Returns rent_estimation dict or None. Never raises.
        """
        key = _build_address(property_dict) or str(property_dict.get("address", ""))
        return await self._fetch_and_parse(property_dict, key)

    async def fetch_and_save_rent_estimate(self, property_id: str, property_dict: dict) -> bool:
        """
        Fetch rent estimate from Rentcast app and save to property.rent_estimation.
        Uses proxy (same as Realtor) and Playwright for JS-rendered content.
        Skips fetch if rent_estimation.fetched_at exists and is within the last 60 days.
        Returns True on success, False on skip/error. Never raises.
        """
        if self.db is not None:
            doc = await self.db.properties_collection.find_one(
                {"property_id": property_id},
                {"rent_estimation.fetched_at": 1},
            )
            re = doc.get("rent_estimation") if isinstance(doc, dict) else None
            fetched_at = re.get("fetched_at") if isinstance(re, dict) else None
            if fetched_at is not None:
                dt = None
                if hasattr(fetched_at, "year"):
                    dt = fetched_at
                elif isinstance(fetched_at, str):
                    try:
                        dt = datetime.fromisoformat(fetched_at.replace("Z", "+00:00"))
                    except Exception:
                        pass
                if dt is not None:
                    if getattr(dt, "tzinfo", None) is not None:
                        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
                    elif not hasattr(dt, "hour"):
                        dt = datetime.combine(dt, datetime.min.time())
                    if (datetime.utcnow() - dt) < timedelta(days=60):
                        logger.debug(
                            "Rentcast: skip property_id=%s, rent_estimation from last 60 days",
                            property_id,
                        )
                        return True
        data = await self._fetch_and_parse(property_dict, str(property_id))
        if data is None:
            logger.debug("Rentcast: no rent or parse failed, skip property_id=%s", property_id)
            return False
        if self.db is None:
            logger.warning("Rentcast: db not set, cannot save property_id=%s", property_id)
            return False
        try:
            await self.db.properties_collection.update_one(
                {"property_id": property_id},
                {"$set": {"rent_estimation": data}},
            )
        except Exception as e:
            logger.warning("Rentcast: db update error for property_id=%s: %s", property_id, e)
            return False
        return True
