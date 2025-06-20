"""High-level asynchronous API client."""

import asyncio, aiohttp
from typing import List, Dict, AsyncIterator, NamedTuple

from .auth import sign_in_email_password, refresh_access_token
from .firestore import AsyncFirestore
from . import auth

PROJECT_ID = "nts-ios-app"
API_BASE = "https://www.nts.live/api/v2"
API_TOKEN = "48fhsdjEK7349fCJBS"
HEADERS = {"Accept": "application/json", "Authorization": f"Basic {API_TOKEN}"}

HISTORY_CF = (
    "https://europe-west2-nts-ios-app.cloudfunctions.net/getHistory"
)

class Favourite(NamedTuple):
    show_alias: str
    created_at: str

class LiveTrackEvent(NamedTuple):
    start_time: str
    artist_names: List[str]
    song_title: str

class ScheduleEvent(NamedTuple):
    now: Dict
    nxt: Dict

class FavouriteEvent(NamedTuple):
    op: str        # "ADDED" or "REMOVED"
    favourite: Favourite

class NTSClient:
    def __init__(self):
        self._access_token = None
        self._refresh_token = None
        self._uid = None
        self._id_token = None
        self._fs: AsyncFirestore | None = None

        # simple in-memory cache: alias -> show_json
        self._show_cache: Dict[str, Dict] = {}

    # ---------- auth ----------
    async def authenticate(self, email: str, password: str):
        self._access_token, self._uid, self._id_token, self._refresh_token = await sign_in_email_password(email, password)
        self._fs = AsyncFirestore(PROJECT_ID, self._access_token)

        # start background refresh
        if self._refresh_token:
            self._token_task = asyncio.create_task(self._access_token_refresher())

    async def _access_token_refresher(self):
        # refresh every 50 minutes
        while True:
            await asyncio.sleep(50 * 60)
            try:
                new_tok = await refresh_access_token(self._refresh_token)
                self._access_token = new_tok
                if self._fs:
                    self._fs._access_token = new_tok  # update firestore instance
            except Exception:
                continue

    # ---------- favourites ----------
    async def fetch_favourites(self) -> List[Favourite]:
        if not self._fs:
            raise RuntimeError("call authenticate() first")
        device_ids = await self._device_ids()
        docs = await self._fs.query_favourites(device_ids)
        favs = [Favourite(show_alias=d["show_alias"].string_value, created_at=d["created_at"].timestamp_value) for d in docs]
        return favs

    async def _device_ids(self) -> List[str]:
        if not self._fs:
            raise RuntimeError("authenticate first")

        return [self._uid]

    async def fetch_favourite_episodes(self, limit: int = 50):
        device_ids = await self._device_ids()
        return await self._fs.query_favourite_episodes(device_ids, limit)

    # ---------- favourite changes stream ----------
    async def listen_favourites(self) -> AsyncIterator[FavouriteEvent]:
        """Stream additions and removals of favourites in real time."""
        if not self._fs:
            raise RuntimeError("authenticate() first")

        device_ids = [self._uid]

        async for ev in self._fs.listen_favourites(device_ids):
            op = ev["op"]
            alias_raw = ev["show_alias"]

            # ev may come from added/change (normal) or removal event.
            alias = alias_raw if isinstance(alias_raw, str) else alias_raw.string_value

            created_raw = ev.get("created_at")
            created_at = (
                created_raw.timestamp_value
                if created_raw and hasattr(created_raw, "timestamp_value")
                else None
            )

            yield FavouriteEvent(
                op=op,
                favourite=Favourite(show_alias=alias, created_at=created_at),
            )

    # ---------- high-level helper: favourites with show details ----------
    async def watch_favourites_with_details(self, *, cache: bool = True) -> AsyncIterator[List[Dict]]:
        """Yield a de-duplicated list of favourites enriched with show details.

        The first result is emitted immediately after authentication.  Subsequent
        lists are emitted whenever the underlying favourites collection changes
        (add or remove).  Each element of the list is a dict that merges the
        `/shows` metadata for the alias with the `created_at` timestamp from the
        favourites document.
        """

        if not self._fs:
            raise RuntimeError("authenticate() first")

        # local state
        favourites: Dict[str, str] = {}  # alias -> created_at ISO str

        async def build_payload() -> List[Dict]:
            if not favourites:
                return []
            aliases = list(favourites.keys())
            details = await self.fetch_show_details(aliases, use_cache=cache)
            out = []
            for alias in aliases:
                show = details.get(alias, {"show_alias": alias})
                show["created_at"] = favourites[alias]
                out.append(show)
            return out

        # initial load
        for fav in await self.fetch_favourites():
            favourites.setdefault(fav.show_alias, fav.created_at)

        prev_aliases = set(favourites.keys())
        yield await build_payload()

        # listen for changes and refresh incrementally
        async for ev in self.listen_favourites():
            if ev.op == "ADDED":
                favourites[ev.favourite.show_alias] = ev.favourite.created_at
            elif ev.op == "REMOVED":
                favourites.pop(ev.favourite.show_alias, None)

            current_aliases = set(favourites.keys())
            if current_aliases == prev_aliases:
                # No real change (e.g., duplicate document_change during initial sync)
                continue

            prev_aliases = current_aliases
            yield await build_payload()

    # ---------- live tracks ----------
    async def listen_live_tracks(self, channel: str, *, initial_snapshot: bool = True) -> AsyncIterator[LiveTrackEvent]:
        if not self._fs:
            raise RuntimeError("authenticate() first")
        pathname = "/stream" if channel == "1" else "/stream2"
        async for doc in self._fs.listen_live_tracks(pathname, initial_snapshot=initial_snapshot):
            artist_names = [v.string_value for v in doc["artist_names"].array_value.values]
            yield LiveTrackEvent(
                start_time=doc["start_time"].timestamp_value,
                artist_names=artist_names,
                song_title=doc["song_title"].string_value,
            )

    async def listen_history(self) -> AsyncIterator[Dict]:
        if not self._fs:
            raise RuntimeError("authenticate() first")
        device_ids = [self._uid]
        async for ev in self._fs.listen_archive_plays(device_ids):
            yield ev

    # ---------- schedule polling ----------
    async def poll_schedule(self, channel: str, interval: int = 50) -> AsyncIterator[ScheduleEvent]:
        prev_id = None
        async with aiohttp.ClientSession() as sess:
            while True:
                async with sess.get(f"{API_BASE}/live", headers=HEADERS) as resp:
                    data = await resp.json()
                now, nxt = None, None
                for ch in data.get("results", []):
                    if ch.get("channel_name") == channel:
                        now = ch.get("now", {})
                        nxt = ch.get("next", {})
                        break
                now_id = (now or {}).get("embeds", {}).get("details", {}).get("episode_alias") or (now or {}).get("broadcast_title")
                if now_id != prev_id:
                    yield ScheduleEvent(now=now, nxt=nxt)
                    prev_id = now_id
                await asyncio.sleep(interval) 

    # ---------- show details ----------
    async def fetch_show_details(
        self,
        aliases: list[str],
        *,
        use_cache: bool = True,
        max_concurrency: int = 25,
    ) -> dict[str, dict]:
        """Return show metadata for the given aliases.

        Behaviour:
        • By default results are cached in-memory so repeat calls for the same
          aliases are instant.
        • `use_cache=False` bypasses the cache and forces fresh network calls.
        • Handles the `/shows` endpoint limit (≈12 aliases per request) by
          batching and merges responses.
        """

        if not aliases:
            return {}

        # Serve from cache if possible and allowed
        if use_cache:
            cached_subset = {a: self._show_cache[a] for a in aliases if a in self._show_cache}
            missing = [a for a in aliases if a not in cached_subset]
        else:
            cached_subset = {}
            missing = list(dict.fromkeys(aliases))  # preserve order

        # Fetch any missing aliases from the API
        fetched: dict[str, dict] = {}
        if missing:
            # The public API appears to accept only a single alias per request
            # in many cases. We therefore fetch each show individually but do so
            # concurrently with a small worker pool for speed.

            SEM_LIMIT = max_concurrency

            async def fetch_one(alias: str, sess: aiohttp.ClientSession, sem: asyncio.Semaphore):
                url = f"{API_BASE}/shows/{alias}"
                async with sem:
                    try:
                        async with sess.get(url, headers=HEADERS, timeout=10) as resp:
                            if resp.status != 200:
                                return None, None
                            data = await resp.json()
                            return alias, data if isinstance(data, dict) else None
                    except Exception:
                        return None, None

            async with aiohttp.ClientSession() as sess:
                sem = asyncio.Semaphore(SEM_LIMIT)
                tasks = [fetch_one(a, sess, sem) for a in missing]
                for coro in asyncio.as_completed(tasks):
                    alias, data = await coro
                    if alias and data and data.get("show_alias"):
                        fetched[alias] = data

        # Update cache
        if use_cache and fetched:
            self._show_cache.update(fetched)

        # Combine results preserving requested order
        combined: dict[str, dict] = {}
        for alias in aliases:
            if alias in cached_subset:
                combined[alias] = cached_subset[alias]
            elif alias in fetched:
                combined[alias] = fetched[alias]

        return combined