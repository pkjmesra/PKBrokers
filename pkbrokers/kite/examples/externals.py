# -*- coding: utf-8 -*-
"""
The MIT License (MIT)

Copyright (c) 2023 pkjmesra

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

"""

"""
This module contains examples of how external consumers 
would use this pkbrokers.kite package. The examples are not 
meant to be run as-is, but rather to illustrate the intended 
usage of the package. For example, PKScreener might use the 
kite_fetch_save_pickle function to fetch and save instrument 
data, while PKAuth might use the kite_auth function to 
authenticate with the Kite API. These examples are meant 
to be simple and straightforward, and may not include all 
necessary error handling or edge cases for production use.
"""
def kite_fetch_save_pickle():
    from pkbrokers.kite.datamanager import InstrumentDataManager

    manager = InstrumentDataManager()
    success = manager.execute(fetch_kite=False, skip_db=True)

    if success:
        print("Saved instrument data into the pickle file")
    else:
        print("Failed to load or create instrument data")
    return success

def kite_auth():
    # Configuration - load from environment in production
    import os
    from PKDevTools.classes.Environment import PKEnvironment

    from pkbrokers.kite.authenticator import KiteAuthenticator

    local_secrets = PKEnvironment().allSecrets
    credentials = {
        "api_key": "kitefront",
        "username": os.environ.get(
            "KUSER", local_secrets.get("KUSER", "You need your Kite username")
        ),
        "password": os.environ.get(
            "KPWD", local_secrets.get("KPWD", "You need your Kite password")
        ),
        "totp": os.environ.get(
            "KTOTP", local_secrets.get("KTOTP", "You need your Kite TOTP")
        ),
    }
    authenticator = KiteAuthenticator(timeout=10)
    authenticator.get_enctoken(**credentials)
