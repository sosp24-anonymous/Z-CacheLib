# Z-CacheLib

Project is cloned from <https://github.com/facebook/CacheLib>

## Building and installation

1. You should install [libzbd](https://github.com/westerndigitalcorporation/libzbd) and [libexplain](https://packages.ubuntu.com/jammy/libexplain-dev) at first.

2. Follow the steps provided by [CacheLib](https://cachelib.org/)

```bash
git clone https://github.com/sosp24-anonymous/Z-CacheLib
cd Z-CacheLib
./contrib/build.sh -j -T

# The resulting library and executables:
./opt/cachelib/bin/cachebench --help
```

## Run CacheBench

You should change device path in config file appropiately before you run the CacheBench.

```bash
./opt/cachelib/bin/cachebench --json_test_config ./configs/kvcache_l2_wc/config.json --progress_stats_file=/tmp/mc-l2-reg.log --logging=INFO
```

More configuration files can be found in `./configs/`.

There are some new config for Z-CacheLib:

1. `navyUseZns`: indicate the device is a ZNS SSD.
2. `navyZnsZoneNum`: set the number of zone to use.
3. `navyZnsDrop`: enable `ZNS Drop` feature.
4. `navyZnsDirect`: enable `ZNS Direct` feature.

And there are detials you should notice:

If you are using `ZNS Direct`:

1. the `navyZnsZoneNum` should be `0`.
2. the `deviceMaxWriteSize` should be `262144`.
3. the `navyRegionSizeMB` should be the size of zone (`1077` in ZN540).

If you are using `ZNS Middle` or `ZNS Drop`, the `deviceMaxWriteSize` should be `0`.
