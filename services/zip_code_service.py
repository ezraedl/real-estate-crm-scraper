import csv
import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

logger = logging.getLogger(__name__)

_LOCATION_PATTERN = re.compile(
    r"^(?P<city>.+?),\s*(?P<state>[A-Za-z]{2})(?:\s+(?P<zip>\d{5})(?:-\d{4})?)?$"
)


def _default_csv_path() -> Path:
    base_dir = Path(__file__).resolve().parents[1]
    return base_dir.parent / "real-estate-crm-backend" / "US ZIP CODES.csv"


def _normalize_city(value: str) -> str:
    return re.sub(r"\s+", " ", value.strip().lower())


@dataclass
class ZipLocation:
    city: str
    state: str
    zip_codes: List[str]

    @property
    def label(self) -> str:
        return f"{self.city}, {self.state}"


class ZipCodeService:
    """Load and provide lookup utilities for U.S. ZIP codes."""

    def __init__(self, csv_path: Optional[Path] = None) -> None:
        env_path = os.getenv("ZIP_CODES_CSV_PATH")
        if env_path:
            self._csv_path = Path(env_path)
        elif csv_path:
            self._csv_path = Path(csv_path)
        else:
            self._csv_path = _default_csv_path()

        self._city_state_to_zips: Dict[Tuple[str, str], List[str]] = {}
        self._city_state_display: Dict[Tuple[str, str], str] = {}
        self._zip_to_city_state: Dict[str, Tuple[str, str]] = {}
        self._loaded = False

    def _ensure_loaded(self) -> None:
        if self._loaded:
            return

        if not self._csv_path.exists():
            logger.warning("ZIP code CSV not found at %s", self._csv_path)
            self._loaded = True
            return

        try:
            with self._csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
                reader = csv.DictReader(handle)
                for row in reader:
                    zip_code = row.get("zip")
                    city = row.get("city")
                    state = row.get("state_id")
                    if not zip_code or not city or not state:
                        continue

                    zip_code = str(zip_code).zfill(5)
                    state_key = state.strip().upper()
                    city_display = city.strip()
                    city_key = _normalize_city(city_display)
                    key = (city_key, state_key)

                    entry = self._city_state_to_zips.setdefault(key, [])
                    if zip_code not in entry:
                        entry.append(zip_code)

                    self._city_state_display.setdefault(key, city_display)
                    self._zip_to_city_state[zip_code] = (city_display, state_key)

            for zip_list in self._city_state_to_zips.values():
                zip_list.sort()

            logger.info(
                "Loaded %d city/state ZIP mappings from %s",
                len(self._city_state_to_zips),
                self._csv_path,
            )
        except Exception as exc:
            logger.error("Failed to load ZIP code CSV: %s", exc)

        self._loaded = True

    def get_zip_codes(self, city: str, state: str) -> List[str]:
        self._ensure_loaded()
        if not city or not state:
            return []
        key = (_normalize_city(city), state.strip().upper())
        return list(self._city_state_to_zips.get(key, []))

    def get_city_state_for_zip(self, zip_code: str) -> Optional[Tuple[str, str]]:
        self._ensure_loaded()
        if not zip_code:
            return None
        return self._zip_to_city_state.get(str(zip_code).zfill(5))

    def parse_location(self, location: str) -> Optional[Tuple[str, str, Optional[str]]]:
        if not location:
            return None
        match = _LOCATION_PATTERN.match(location.strip())
        if not match:
            return None
        city = match.group("city").strip()
        state = match.group("state").strip().upper()
        zip_code = match.group("zip")
        return city, state, zip_code

    def expand_location(
        self,
        location: str,
        batch_size: Optional[int] = None,
    ) -> List[Dict[str, Iterable[str]]]:
        parsed = self.parse_location(location)
        if not parsed:
            return [{"locations": [location], "label": location, "split": False}]

        city, state, zip_code = parsed
        if zip_code:
            label = f"{city}, {state} {zip_code}"
            return [{"locations": [label], "label": label, "split": False}]

        zip_codes = self.get_zip_codes(city, state)
        if not zip_codes:
            logger.warning(
                "No ZIP codes found for %s, %s; falling back to original location",
                city,
                state,
            )
            return [{"locations": [location], "label": location, "split": False}]

        if not batch_size or batch_size <= 1:
            return [
                {
                    "locations": [f"{city}, {state} {zip_code}"],
                    "label": f"{city}, {state} {zip_code}",
                    "split": True,
                }
                for zip_code in zip_codes
            ]

        chunks: List[Dict[str, Iterable[str]]] = []
        total = len(zip_codes)
        for idx in range(0, total, batch_size):
            chunk = zip_codes[idx : idx + batch_size]
            chunk_locations = [f"{city}, {state} {zip_code}" for zip_code in chunk]
            label = f"{city}, {state} ({len(chunk)} ZIPs)"
            chunks.append({"locations": chunk_locations, "label": label, "split": True})
        return chunks

    def search_locations(
        self,
        query: str,
        *,
        state: Optional[str] = None,
        limit: int = 10,
    ) -> List[ZipLocation]:
        self._ensure_loaded()
        if not query:
            return []

        normalized_query = _normalize_city(query)
        state_filter = state.strip().upper() if state else None

        matches: List[Tuple[int, ZipLocation]] = []
        for (city_key, state_key), zip_codes in self._city_state_to_zips.items():
            if state_filter and state_key != state_filter:
                continue

            display_city = self._city_state_display.get((city_key, state_key), city_key.title())
            if normalized_query in city_key:
                score = 0 if city_key.startswith(normalized_query) else 1
            elif normalized_query in display_city.lower():
                score = 2
            else:
                continue

            matches.append(
                (
                    score,
                    ZipLocation(
                        city=display_city,
                        state=state_key,
                        zip_codes=list(zip_codes),
                    ),
                )
            )

        matches.sort(key=lambda item: (item[0], item[1].city.lower(), -len(item[1].zip_codes)))
        return [location for _, location in matches[:limit]]


zip_code_service = ZipCodeService()
