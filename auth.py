"""Firebase email/password authentication (async)."""

import aiohttp, asyncio, os
from typing import Tuple

FIREBASE_CONFIG = {
    "apiKey": "AIzaSyA4Qp5AvHC8Rev72-10-_DY614w_bxUCJU",
}

SECURE_TOKEN_URL = "https://securetoken.googleapis.com/v1/token"
IDENTITY_TOOLKIT_URL = "https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword"


async def sign_in_email_password(email: str, password: str) -> Tuple[str, str, str, str]:
    """Return (access_token, uid, id_token, refresh_token) using Firebase Authentication."""
    async with aiohttp.ClientSession() as sess:
        # step 1: email+password -> idToken + refreshToken
        params = {"key": FIREBASE_CONFIG["apiKey"]}
        payload = {
            "email": email,
            "password": password,
            "returnSecureToken": True,
        }
        async with sess.post(f"{IDENTITY_TOOLKIT_URL}", params=params, json=payload) as resp:
            data = await resp.json()
            refresh_token = data["refreshToken"]
            uid = data["localId"]
            id_token = data["idToken"]

        # step 2: refresh token -> Google OAuth2 access_token
        async with sess.post(
            f"{SECURE_TOKEN_URL}?key={FIREBASE_CONFIG['apiKey']}",
            data={"grant_type": "refresh_token", "refresh_token": refresh_token},
        ) as resp:
            tok_data = await resp.json()
            access_token = tok_data["access_token"]

    return access_token, uid, id_token, refresh_token


# ---------------------------------------------

async def refresh_access_token(refresh_token: str) -> str:
    """Exchange a long-lived Firebase refresh token for a new Google OAuth2 access_token."""
    async with aiohttp.ClientSession() as sess:
        async with sess.post(
            f"{SECURE_TOKEN_URL}?key={FIREBASE_CONFIG['apiKey']}",
            data={"grant_type": "refresh_token", "refresh_token": refresh_token},
        ) as resp:
            data = await resp.json()
            return data["access_token"] 