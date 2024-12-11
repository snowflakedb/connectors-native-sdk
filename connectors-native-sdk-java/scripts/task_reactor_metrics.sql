-- Copyright (c) 2024 Snowflake Inc.
SET EVENT_TABLE = 'TOOLS.PUBLIC.EVENTS';
SET APP_NAME = 'NATIVE_SDK_CONNECTOR_TEMPLATE_INSTANCE';

-- TASK_REACTOR_WORKER_IDLE_TIME
SELECT
    event.record:name::string AS EVENT_NAME,
    span.record_attributes:task_reactor_instance::string AS INSTANCE_NAME,
    span.record_attributes:worker_id AS WORKER_ID,
    event.record_attributes:value AS DURATION
FROM IDENTIFIER($EVENT_TABLE) event
         JOIN IDENTIFIER($EVENT_TABLE) span ON event.trace:span_id = span.trace:span_id AND event.record_type = 'SPAN_EVENT' AND span.record_type = 'SPAN'
    WHERE
        event.resource_attributes:"snow.database.name" = $APP_NAME
            AND event.record:name = 'TASK_REACTOR_WORKER_IDLE_TIME'
    ORDER BY event.timestamp DESC;

-- TASK_REACTOR_WORKER_WORKING_TIME
SELECT
    event.record:name::string AS EVENT_NAME,
    span.record_attributes:task_reactor_instance::string AS INSTANCE_NAME,
    span.record_attributes:worker_id AS WORKER_ID,
    event.record_attributes:value AS DURATION
FROM IDENTIFIER($EVENT_TABLE) event
         JOIN IDENTIFIER($EVENT_TABLE) span ON event.trace:span_id = span.trace:span_id AND event.record_type = 'SPAN_EVENT' AND span.record_type = 'SPAN'
    WHERE
        event.resource_attributes:"snow.database.name" = $APP_NAME
            AND event.record:name = 'TASK_REACTOR_WORKER_WORKING_TIME'
    ORDER BY event.timestamp DESC;

-- TASK_REACTOR_WORK_ITEM_WAITING_IN_QUEUE_TIME
SELECT
    event.record:name::string AS EVENT_NAME,
    span.record_attributes:task_reactor_instance::string AS INSTANCE_NAME,
    event.record_attributes:value AS DURATION,
    event.timestamp
FROM IDENTIFIER($EVENT_TABLE) event
        JOIN IDENTIFIER($EVENT_TABLE) span ON event.trace:span_id = span.trace:span_id AND event.record_type = 'SPAN_EVENT' AND span.record_type = 'SPAN'
    WHERE
        event.resource_attributes:"snow.database.name" = $APP_NAME
            AND event.record:name = 'TASK_REACTOR_WORK_ITEM_WAITING_IN_QUEUE_TIME'
    ORDER BY event.timestamp DESC;

-- TASK_REACTOR_WORK_ITEMS_NUMBER_IN_QUEUE
SELECT
    event.record:name::string AS EVENT_NAME,
    event.record_attributes:task_reactor_instance::string AS INSTANCE_NAME,
    event.record_attributes:value AS WORK_ITEMS_NUMBER,
    event.timestamp
FROM IDENTIFIER($EVENT_TABLE) event
    WHERE
        event.resource_attributes:"snow.database.name" = $APP_NAME
            AND event.record:name = 'TASK_REACTOR_WORK_ITEMS_NUMBER_IN_QUEUE'
    ORDER BY event.timestamp DESC;

-- TASK_REACTOR_WORKER_STATUS
SELECT
    event.record:name::string AS EVENT_NAME,
    span.record_attributes:task_reactor_instance::string AS INSTANCE_NAME,
    event.record_attributes:TOTAL AS WORKERS_TOTAL,
    IFNULL(event.record_attributes:AVAILABLE, 0) AS WORKERS_AVAILABLE,
    IFNULL(event.record_attributes:WORK_ASSIGNED, 0) AS WORKERS_WORK_ASSIGNED,
    IFNULL(event.record_attributes:IN_PROGRESS, 0) AS WORKERS_IN_PROGRESS,
    IFNULL(event.record_attributes:SCHEDULED_FOR_CANCELLATION, 0) AS WORKERS_SCHEDULED_FOR_CANCELLATION,
    event.timestamp
FROM IDENTIFIER($EVENT_TABLE) event
    JOIN IDENTIFIER($EVENT_TABLE) span ON event.trace:span_id = span.trace:span_id AND event.record_type = 'SPAN_EVENT' AND span.record_type = 'SPAN'
    WHERE
        event.resource_attributes:"snow.database.name" = $APP_NAME
            AND event.record:name = 'TASK_REACTOR_WORKER_STATUS'
    ORDER BY event.timestamp DESC;
