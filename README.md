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

    favs = await client.fetch_favourites()
    print("favourites", len(favs))

    async for track in client.listen_live_tracks("1"):
        print(track)

asyncio.run(main())
```

## Supported features
* Firebase email/password auth (async)
* Fetch favourite shows (`fetch_favourites`)
* Fetch favourite episodes (`fetch_favourite_episodes`)
* Listen to live track updates (`listen_live_tracks`)
* Poll schedule (`poll_schedule`)
* Stream archive plays / history (`listen_history`)

Everything returns `asyncio`-friendly coroutines or async generators.

---
This project is **unofficial** and not affiliated with NTS Radio. Use at your own risk. 