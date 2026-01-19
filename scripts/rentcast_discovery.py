"""
Discovery script: fetch app.rentcast.io with proxy + browser headers (same as Realtor),
save HTML, and print notes on where rent/low/high/comps might be.
Run from scraper root: python scripts/rentcast_discovery.py
"""

import asyncio
import os
import re
import sys

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)


def _build_proxy_url(proxy):
    """Build proxy_url like MLSScraper.get_proxy_config."""
    if not proxy:
        return None
    import secrets
    username = proxy.username
    if username and "session." not in username:
        username = f"{username};session.{secrets.randbelow(1_000_000)}"
    if username and proxy.password:
        return f"http://{username}:{proxy.password}@{proxy.host}:{proxy.port}"
    return f"http://{proxy.host}:{proxy.port}"


async def main():
    from config import settings
    from proxy_manager import proxy_manager

    url = "https://app.rentcast.io/app?address=851%20W%2025th%20St,%20Indianapolis,%20IN,%2046208"
    out_path = os.path.join(project_root, "temp", "rentcast_discovery.html")

    proxy = proxy_manager.get_next_proxy()
    proxy_url = _build_proxy_url(proxy)
    headers = proxy_manager.get_random_headers()

    print(f"Fetching: {url}")
    print(f"Proxy: {'yes' if proxy_url else 'no'}")
    print(f"User-Agent: {headers.get('User-Agent', '')[:60]}...")

    html = None
    try:
        try:
            from curl_cffi.requests import AsyncSession
            async with AsyncSession(impersonate="chrome") as s:
                resp = await s.get(url, proxy=proxy_url, headers=headers, timeout=30.0)
                resp.raise_for_status()
                html = resp.text
                print("Fetched with curl_cffi (impersonate=chrome)")
        except ImportError:
            import httpx
            async with httpx.AsyncClient(proxy=proxy_url, headers=headers) as c:
                resp = await c.get(url, timeout=30.0)
                resp.raise_for_status()
                html = resp.text
                print("Fetched with httpx")
    except Exception as e:
        print(f"Fetch error: {e}")
        return

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"Saved HTML to {out_path} ({len(html)} chars)")

    # Quick inspect
    if "__NEXT_DATA__" in html:
        print("  -> Found __NEXT_DATA__ (Next.js)")
    if 'type="application/json"' in html or "application/ld+json" in html:
        print("  -> Found script type=application/json or ld+json")
    for pat in [r"\$[\d,]+(?:\.\d{2})?", r'"rent"\s*:\s*[\d.]+', r'rentRangeLow|rent_range_low', r'rentRangeHigh|rent_range_high']:
        if re.search(pat, html, re.I):
            print(f"  -> Pattern found: {pat}")


if __name__ == "__main__":
    asyncio.run(main())
