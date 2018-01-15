mackerel-plugin-nifcloud-rdb
================================

NIFCLOUD RDB custom metrics plugin for mackerel.io agent.


## Usage

```
mackerel-plugin-nifcloud-rdb -identifier=<db-instance-identifier> -access-key-id=<id> secret-access-key=<key> -region=<east-1 or east-2 or east-3 or east-4 or west-1> [-tempfile=<tempfile>] [-metric-key-prefix=<prefix>] [-metric-label-prefix=<label-prefix>]"
```


## Example of mackerel-agent.conf

```
[plugin.metrics.rdb]
command = "/path/to/mackerel-plugin-nifcloud-rdb -identifier=sample -access-key-id=your_access_key -secret-access-key=your_secret_key -region=east-1"
```
