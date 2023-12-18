# sodata-cloud-native

```
jbang converter.java &> foo.log
DATA_DIR=/Users/stefan/tmp/cloud-native/ CREATE_STATIC_DATASETS=true jbang converter.java
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

crontab:
```
SHELL=/bin/bash
PATH=/root/.sdkman/candidates/jbang/current/bin:/root/.sdkman/candidates/java/current/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin

DATA_DIR=/Users/stefan/tmp/cloud-native/ CREATE_STATIC_DATASETS=true jbang converter.java
```

```
java -jar /Users/stefan/apps/ili2pg-4.9.1/ili2pg-4.9.1.jar --dbhost localhost --dbport 54322 --dbdatabase pub --dbusr ddluser --dbpwd ddluser --dbschema agi_stac_v1 --models SO_AGI_STAC_20230426 --strokeArcs --coalesceJson --importBatchSize 5000 --schemaimport
```

```
java -jar /Users/stefan/apps/ili2pg-4.9.1/ili2pg-4.9.1.jar --dbhost localhost --dbport 54322 --dbdatabase pub --dbusr ddluser --dbpwd ddluser --dbschema agi_stac_v1 --models SO_AGI_STAC_20230426 --strokeArcs --coalesceJson --importBatchSize 5000 --deleteData --import meta.xtf
```

Hetzner: Firewall erstellt wegen DB.