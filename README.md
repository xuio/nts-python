# NTS Radio Async API

Unofficial, async Python Module for the public and Firestore-backed parts of [NTS Radio](https://www.nts.live).

## Install
```bash
pip install -r requirements.txt  # inside this folder
```

## Quick example
```python
import asyncio
from nts_radio_async_api import NTSClient

async def main():
    client = NTSClient()
    await client.authenticate("email", "password")

    # 1) subscribe to favourites list (updates automatically)
    async for favourites in client.watch_favourites_with_details():
        print("You now have", len(favourites), "favourites →", [s["show_alias"] for s in favourites])

    # 2) fetch metadata for an ad-hoc list (cached by default)
    shows = await client.fetch_show_details(["ok-williams", "peach"])  # served from cache next time
    print(shows["ok-williams"]["name"])

    # 3) listen for what's playing live on channel 1
    async for track in client.listen_live_tracks("1"):
        print(track)

asyncio.run(main())
```

## Supported features
* Firebase email/password auth (async)
* Fetch favourite shows (`fetch_favourites`)
* Fetch favourite episodes (`fetch_favourite_episodes`)
* Fetch show metadata (`fetch_show_details`)  
  * in-memory caching (on by default)  
  * `use_cache=False` to bypass, `max_concurrency=N` to tune parallel fetches
* Live favourites stream (`listen_favourites`) and convenience helper `watch_favourites_with_details`
* Listen to live track updates (`listen_live_tracks`)
* Poll schedule (`poll_schedule`)
* Stream archive plays / history (`listen_history`)

Everything returns `asyncio`-friendly coroutines or async generators.

---
This project is **unofficial** and not affiliated with NTS Radio. Use at your own risk. 