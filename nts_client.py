"""High-level asynchronous API client."""

import asyncio, aiohttp
from typing import List, Dict, AsyncIterator, NamedTuple

from .auth import sign_in_email_password
from .firestore import AsyncFirestore

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


class NTSClient:
    def __init__(self):
        self._access_token = None
        self._uid = None
        self._id_token = None
        self._fs: AsyncFirestore | None = None

    # ---------- auth ----------
    async def authenticate(self, email: str, password: str):
        self._access_token, self._uid, self._id_token = await sign_in_email_password(email, password)
        self._fs = AsyncFirestore(PROJECT_ID, self._access_token)

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

    # ---------- live tracks ----------
    async def listen_live_tracks(self, channel: str) -> AsyncIterator[LiveTrackEvent]:
        if not self._fs:
            raise RuntimeError("authenticate() first")
        pathname = "/stream" if channel == "1" else "/stream2"
        async for doc in self._fs.listen_live_tracks(pathname):
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