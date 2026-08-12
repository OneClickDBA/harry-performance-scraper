# Recreate oracle load test environment

Select Oracle instance

```bash
export INSTANCE="second26ai"
```

Copy `HR` schema db samples files to container:

```bash
cd ~/harry-performance-scraper/db-sample-schemas-23.3/human_resources/
for i in hr_*sql ; do docker cp ${i} "${INSTANCE}":/ ; done
```

Load it into the database:

```bash
docker exec -it "${INSTANCE}" bash
sqlplus '/ as sysdba'
alter session set container=FREEPDB1;
@/hr_install.sql
```

Launch load test

```bash
cd ~/harry-performance-scraper/db-sample-schemas-23.3/human_resources/load_test
bash run_load_test.sh  --container "${INSTANCE}" --setup
```
