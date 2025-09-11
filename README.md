A simple python library to connect to various brokers using standard user credentials, fetch the instruments (trading symbols), indices and their ticks during as well as off market hours.

[![codecov][codecov-badge]][codecov]

[codecov-badge]: https://codecov.io/gh/pkjmesra/PKBrokers/branch/main/graph/badge.svg
[codecov]: https://codecov.io/gh/pkjmesra/PKBrokers
![Title](https://raw.githubusercontent.com/pkjmesra/pkbrokers/main/screenshots/logos/Logo8_1.png)

# pkbrokers (# 1 Stock screener/scanner for NSE India)
| [![MADE-IN-INDIA][MADE-IN-INDIA-badge]][MADE-IN-INDIA] | [![GitHub release (latest by date)][GitHub release (latest by date)-badge]][GitHub release (latest by date)] | [![Downloads][Downloads-badge]][Downloads] | ![latest download][Latest-Downloads-badge]  | [![Docker Pulls][Docker Pulls-badge]][Docker Status] |
| :-------------: | :-----------------: | :-----------------: | :-----------------: | :-----------------: |
| Platforms | [![Windows][Windows-badge]][Windows] | [![Linux(x64)][Linux-badge_x64]][Linux_x64] [![Linux(arm64)][Linux-badge_arm64]][Linux_arm64] | [![Mac OS(x64)][Mac OS-badge_x64]][Mac OS_x64] [![Mac OS(arm64)][Mac OS-badge_arm64]][Mac OS_arm64] | [![Docker Status][Docker Status-badge]][Docker Status] |
| Package / Docs | [![Documentation][Documentation-badge]][Documentation] [![OpenSSF Best Practices][OpenSSF-Badge]][OpenSSF-pkbrokers]  | [![PyPI][pypi-badge]][pypi] | [![is wheel][wheel-badge]][pypi] | ![github license][github-license] |
| Tests/Code-Quality | [![CodeFactor][Codefactor-badge]][Codefactor] | <a href="https://coveralls.io/github/pkjmesra/pkbrokers?branch=main"><img src="https://coveralls.io/repos/github/pkjmesra/pkbrokers/badge.svg?branch=main"></a> | [![codecov][codecov-badge]][codecov] | [![After Market][After Market-badge]][After Market] |


[![Star History Chart](https://api.star-history.com/svg?repos=pkjmesra/pkbrokers&type=Date)](https://star-history.com/#pkjmesra/pkbrokers&Date)

[MADE-IN-INDIA-badge]: https://img.shields.io/badge/MADE%20WITH%20%E2%9D%A4%20IN-INDIA-orange
[MADE-IN-INDIA]: https://en.wikipedia.org/wiki/India
[Windows-badge]: https://img.shields.io/badge/Windows-0078D6?logo=windows&logoColor=white
[Windows]: https://github.com/pkjmesra/pkbrokers/releases/download/0.1.20250911.16/pkkite.exe
[Linux-badge_x64]: https://img.shields.io/badge/Linux(x64)-FCC624?logo=linux&logoColor=black
[Linux_x64]: https://github.com/pkjmesra/pkbrokers/releases/download/0.1.20250911.16/pkkite_x64.bin
[Linux-badge_arm64]: https://img.shields.io/badge/Linux(arm64)-FCC624?logo=linux&logoColor=black
[Linux_arm64]: https://github.com/pkjmesra/pkbrokers/releases/download/0.1.20250911.16/pkkite_arm64.bin
[Mac OS-badge_x64]: https://img.shields.io/badge/mac%20os(x64)-D3D3D3?logo=apple&logoColor=000000
[Mac OS_x64]: https://github.com/pkjmesra/pkbrokers/releases/download/0.1.20250911.16/pkkite_x64.run
[Mac OS-badge_arm64]: https://img.shields.io/badge/mac%20os(arm64)-D3D3D3?logo=apple&logoColor=000000
[Mac OS_arm64]: https://github.com/pkjmesra/pkbrokers/releases/download/0.1.20250911.16/pkkite_arm64.run
[GitHub release (latest by date)-badge]: https://img.shields.io/github/v/release/pkjmesra/pkbrokers
[GitHub release (latest by date)]: https://github.com/pkjmesra/pkbrokers/releases/latest
[pypi-badge]: https://img.shields.io/pypi/v/pkbrokers.svg?style=flat-square
[pypi]: https://pypi.python.org/pypi/pkbrokers
[wheel-badge]: https://img.shields.io/pypi/wheel/pkbrokers.svg?style=flat-square
[GitHub all releases]: https://img.shields.io/github/downloads/pkjmesra/pkbrokers/total?color=Green&label=Downloads&style=for-the-badge
[License-badge]: https://img.shields.io/github/license/pkjmesra/pkbrokers?style=for-the-badge
[License]: https://github.com/pkjmesra/pkbrokers/blob/main/LICENSE
[Codefactor-badge]: https://www.codefactor.io/repository/github/pkjmesra/pkbrokers/badge
[Codefactor]: https://www.codefactor.io/repository/github/pkjmesra/pkbrokers
[PR-Guidelines-badge]: https://img.shields.io/badge/PULL%20REQUEST-GUIDELINES-red?style=for-the-badge
[PR-Guidelines]: https://github.com/pkjmesra/pkbrokers/blob/new-features/CONTRIBUTING.md
[github-license]: https://img.shields.io/github/license/pkjmesra/pkbrokers
[Downloads-badge]: https://static.pepy.tech/personalized-badge/pkbrokers?period=total&units=international_system&left_color=black&right_color=brightgreen&left_text=Total%20Downloads
[Downloads]: https://pepy.tech/project/pkbrokers
[Latest-Downloads-badge]: https://img.shields.io/github/downloads-pre/pkjmesra/pkbrokers/latest/total?logo=github
[Coverage-Status-badge]: https://coveralls.io/repos/github/pkjmesra/pkbrokers/badge.svg?kill_cache=1
[Coverage-Status]: https://coveralls.io/github/pkjmesra/pkbrokers?branch=main
[codecov-badge]: https://codecov.io/gh/pkjmesra/pkbrokers/branch/main/graph/badge.svg
[codecov]: https://codecov.io/gh/pkjmesra/pkbrokers
[Documentation-badge]: https://readthedocs.org/projects/pkbrokers/badge/?version=latest
[Documentation]: https://pkbrokers.readthedocs.io/en/latest/?badge=latest
[Docker Status-badge]: https://img.shields.io/docker/automated/pkjmesra/pkbrokers.svg
[Docker Status]: https://hub.docker.com/repository/docker/pkjmesra/pkbrokers
[Docker Pulls-badge]: https://img.shields.io/docker/pulls/pkjmesra/pkbrokers.svg
[Prod Scan Tests-badge]: https://github.com/pkjmesra/pkbrokers/actions/workflows/w5-workflow-prod-scans_Tests.yml/badge.svg
[Prod Scan Tests]: https://github.com/pkjmesra/pkbrokers/actions/workflows/w5-workflow-prod-scans_Tests.yml
[After Market-badge]: https://github.com/pkjmesra/pkbrokers/actions/workflows/w9-workflow-download-data.yml/badge.svg
[After Market]: https://github.com/pkjmesra/pkbrokers/actions/workflows/w9-workflow-download-data.yml
[New Features-badge]: https://github.com/pkjmesra/pkbrokers/actions/workflows/w10-workflow-features-test.yml/badge.svg
[New Features]: https://github.com/pkjmesra/pkbrokers/actions/workflows/w10-workflow-features-test.yml
[New Release-badge]: https://github.com/pkjmesra/pkbrokers/actions/workflows/w1-workflow-build-matrix.yml/badge.svg
[New Release]: https://github.com/pkjmesra/pkbrokers/actions/workflows/w1-workflow-build-matrix.yml
[Docker-Build-badge]: https://github.com/pkjmesra/pkbrokers/actions/workflows/w15-docker-image.yml/badge.svg
[Docker-Build]: https://github.com/pkjmesra/pkbrokers/actions/workflows/w15-docker-image.yml
[OpenSSF-Badge]:https://www.bestpractices.dev/projects/10011/badge
[OpenSSF-pkbrokers]: https://www.bestpractices.dev/projects/10011
