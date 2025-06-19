"""Async Python SDK for the public + Firestore-backed parts of NTS Radio.

Example
-------
```python
import asyncio
from nts_radio_async_api import NTSClient

async def main():
    client = NTSClient()
    await client.authenticate(email="you@example.com", password="secret")

    favs = await client.fetch_favourites()
    print("found", len(favs))

    # live tracks
    async for track in client.listen_live_tracks(channel="1"):
        print(track)

asyncio.run(main())
```
"""

from .nts_client import NTSClient, LiveTrackEvent, ScheduleEvent, Favourite

__all__ = [
    "NTSClient",
    "LiveTrackEvent",
    "ScheduleEvent",
    "Favourite",
] 