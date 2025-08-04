# -*- coding: utf-8 -*-
# """
#     The MIT License (MIT)

#     Copyright (c) 2023 pkjmesra

#     Permission is hereby granted, free of charge, to any person obtaining a copy
#     of this software and associated documentation files (the "Software"), to deal
#     in the Software without restriction, including without limitation the rights
#     to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#     copies of the Software, and to permit persons to whom the Software is
#     furnished to do so, subject to the following conditions:

#     The above copyright notice and this permission notice shall be included in all
#     copies or substantial portions of the Software.

#     THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#     IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#     FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#     AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#     LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#     OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
#     SOFTWARE.

# """
import requests
import os
import uuid
import pyotp


from kiteconnect import KiteConnect

def get_request_token(credentials: dict) -> str:
    """Use provided credentials and return request token.
    Args:
        credentials: Login credentials for Kite
    Returns:
        Request token for the provided credentials
    """

    kite = KiteConnect(api_key=credentials["api_key"])
    # 1. Initial request to get cookies
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'X-Kite-Version': '3.0.0'
    }
    session = requests.Session()
    response = session.get(kite.login_url(), headers=headers)

    # User login POST request
    login_payload = {
        "user_id": credentials["username"],
        "password": credentials["password"],
        "type": "user_id"
    }
    login_response = session.post("https://kite.zerodha.com/api/login", data=login_payload, headers=headers)
    login_data = login_response.json()

    # 3. TOTP request with all required headers
    totp_headers = {
        **headers,
        'Origin': 'https://kite.zerodha.com',
        'Referer': 'https://kite.zerodha.com/',
        'Content-Type': 'application/x-www-form-urlencoded',
        'X-Kite-Userid': credentials["username"].upper(),
        'X-Kite-App-Uuid': str(uuid.uuid4())  # Generate a random UUID
    }
    # TOTP POST request
    totp_payload = {
        "user_id": credentials["username"],
        "request_id": login_data["data"]["request_id"],
        "twofa_value": pyotp.TOTP(credentials["totp"], interval=int(30)).now(), #otp.get_totp(credentials["totp_key"]),
        "twofa_type": "totp",
        "skip_session": None,
    }
    totp_response = session.post("https://kite.zerodha.com/api/twofa", data=totp_payload, headers=totp_headers)
    cookies = totp_response.headers.get("Set-Cookie",None)
    enc_token = ""
    if cookies is not None:
        all_cookies = cookies.split(";")
        for cookie in all_cookies:
            if "enctoken" in cookie:
                cookie_parts = cookie.strip().split(",")
                for cookie_part in cookie_parts:
                    if "enctoken" in cookie_part:
                        enc_token = cookie_part.strip().replace("enctoken=","")
    return enc_token

if __name__ == "__main__":
    # Configuration - load from environment in production
    from dotenv import dotenv_values
    local_secrets = dotenv_values(".env.dev")

    credentials = {
                    "api_key" : "kitefront",
                    "username" : os.environ.get("KUSER",local_secrets.get("KUSER","You need your Kite username")),
                    "password" : os.environ.get("KPWD",local_secrets.get("KPWD","You need your Kite password")),
                    "totp" : os.environ.get("KTOTP",local_secrets.get("KTOTP","You need your Kite TOTP")),
                   }
    req_token = get_request_token(credentials)