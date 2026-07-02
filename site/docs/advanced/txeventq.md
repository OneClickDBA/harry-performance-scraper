---
title: Monitoring TxEventQ
sidebar_position: 2
---

# Monitoring Transactional Event Queues

[Oracle Transactional Event Queues](https://docs.oracle.com/en/database/oracle/oracle-database/23/adque/index.html) ("TxEventQ") is a fault-tolerant, scalable, real-time messaging backbone offered by converged Oracle AI Database that allows you to build an enterprise-class event-driven architectures.

Access to the real-time broker, producer, and consumer metrics in a single dashboard and receiving alerts for issues allows teams to understand the state of their system.

The repository includes custom metric definitions for monitoring TxEventQ.

> Note: The metrics are written for Oracle AI Database 21c or later.

### How to create some traffic with PL/SQL

If you need to create a topic to monitor, you can use these statements to create and start a topic, and create a subscriber:

```sql
declare
    subscriber sys.aq$_agent;
begin
  -- create the topic
  dbms_aqadm.create_transactional_event_queue(
    queue_name => 'my_topic',
    multiple_consumers => true  -- true makes a pub/sub topic
  );

  -- start the topic
  dbms_aqadm.start_queue(
    queue_name => 'my_topic'
  );

  -- create a subscriber
  dbms_aqadm.add_subscriber(
    queue_name => 'my_teq',
    subscriber => sys.aq$_agent(
      'my_subscriber',    -- the subscriber name
      null,               -- address, only used for notifications
      0                   -- protocol
    ),
    rule => 'correlation = ''my_subscriber'''
  );
end;
```

You can produce a message with these commands:

```sql
declare
    enqueue_options    dbms_aq.enqueue_options_t;
    message_properties dbms_aq.message_properties_t;
    message_handle     raw(16);
    message            SYS.AQ$_JMS_TEXT_MESSAGE;
begin
    -- create the message payload
    message := SYS.AQ$_JMS_TEXT_MESSAGE.construct;
    message.set_text('{"orderid": 12345, "username": "Jessica Smith"}');

    -- set the consumer name
    message_properties.correlation := 'my_subscriber';

    -- enqueue the message
    dbms_aq.enqueue(
        queue_name           => 'my_topic',
        enqueue_options      => enqueue_options,
        message_properties   => message_properties,
        payload              => message,
        msgid                => message_handle);

    -- commit the transaction
    commit;
end;
```

### How to create some traffic with Java (Spring Boot)

A simple load generator is provided in [this directory](https://github.com/dodger-one/oracledb-performance-scraper/tree/main/docker-compose/txeventq-load) which you can use to create some traffic so you can experiment with the sample dashboard.

To run the sample, set `DB_PASSWORD` and, if needed, override `DB_URL` or `DB_USERNAME` for your environment before starting the application.  The default URL targets a local database at `localhost:1521/freepdb1`.

```bash
export DB_PASSWORD='<your-demo-password>'
mvn spring-boot:run
```

The application will create ten queues names TOPIC_0 through TOPIC_9 and randomly produce and consume messages on those queues.  The example dashboard shown below was monitoring traffic produced using this application.

### Metrics definitions

The metric definitions are provided in `custom-metrics-example/txeventq-metrics.toml`. Provide this file to the scraper by adding it to your container image, mounting it on the server, or creating a Kubernetes config map. Reference the mounted file from the scraper configuration under `metrics.custom`.

### Additional database permissions

The database user that the scraper uses to connect to the database will also need additional permissions, which can be granted with these statements. This example assumes the scraper connects with the username `scraper`:

```sql
grant execute on dbms_aq to scraper;
grant execute on dbms_aqadm to scraper;
grant execute on dbms_aqin to scraper;
grant execute on dbms_aqjms_internal to scraper;
grant execute on dbms_teqk to scraper;
grant execute on DBMS_RESOURCE_MANAGER to scraper;
grant select_catalog_role to scraper;
grant select on sys.aq$_queue_shards to scraper;
grant select on user_queue_partition_assignment_table to scraper;
```

### Grafana dashboard

A Grafana dashboard for Transactional Event Queues is still a legacy Prometheus dashboard. It is not part of the PostgreSQL-backed dashboard set until it is migrated to read from `oracle_metric_samples`.

> Note:  You may not see any activity on the dashboard unless there are clients producing and consuming messages from topics.

The dashboard will look like this:

![Oracle AI Database Dashboard](/img/txeventq-dashboard-v2.png)
