# sodata-cloud-native

```
jbang converter.java &> foo.log
```

```
docker build -t sogis/sodata-cloud-native .
```

```
docker run --rm -v /Users/stefan/tmp/cloud-native:/opt/data:ro -p 8080:8080 sogis/sodata-cloud-native 
```

``````
docker run --rm -v /data:/opt/data:ro -p 8080:8080 sogis/sodata-cloud-native 
```

## Server

```
apt-get update
apt-get install zip unzip
```

```
curl -s "https://get.sdkman.io" | bash
```

```
source "$HOME/.sdkman/bin/sdkman-init.sh"
```

```
sdk i jbang 0.114.0
```

```
sdk i java 17.0.9-tem
```

```
mkdir /data
```

```
sudo systemctl enable cron
```
