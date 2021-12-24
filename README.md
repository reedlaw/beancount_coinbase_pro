# Beancount importer for Coinbase Pro

## Usage
Add repo to `importers/` directory.

Add importer to import configuration file (e.g. `personal.import`).
```
from importers import CoinbasePro

CONFIG = [
    CoinbasePro.Importer("USD", "Assets:Coinbase-Pro"),
]
```

Check with bean-identify:

```
bean-identify personal.import ./downloads
```

Import transactions with bean-extract:

```
bean-extract personal.import ./downloads > coinbase.beancount
```
