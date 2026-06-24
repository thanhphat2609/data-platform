from pyspark.sql.types import (
    DateType, LongType,
    StringType, StructField, StructType, TimestampType,
)

__all__ = [
    "_users_schema",
    "_projects_schema",
    "_tasks_schema",
    "_issues_schema",
    "_comments_schema",
]

_users_schema = StructType([
    StructField("id",             LongType(),      True),
    StructField("username",       StringType(),    True),
    StructField("email",          StringType(),    True),
    StructField("role",           StringType(),    True),
    StructField("created_at",     TimestampType(), True),
    StructField("ingestion_date", DateType(),      True),
])

_projects_schema = StructType([
    StructField("id",             LongType(),      True),
    StructField("name",           StringType(),    True),
    StructField("description",    StringType(),    True),
    StructField("owner_id",       LongType(),      True),
    StructField("status",         StringType(),    True),
    StructField("start_date",     DateType(),      True),
    StructField("end_date",       DateType(),      True),
    StructField("created_at",     TimestampType(), True),
    StructField("ingestion_date", DateType(),      True),
])

_tasks_schema = StructType([
    StructField("id",             LongType(),      True),
    StructField("project_id",     LongType(),      True),
    StructField("title",          StringType(),    True),
    StructField("description",    StringType(),    True),
    StructField("assignee_id",    LongType(),      True),
    StructField("status",         StringType(),    True),
    StructField("priority",       StringType(),    True),
    StructField("due_date",       DateType(),      True),
    StructField("created_at",     TimestampType(), True),
    StructField("ingestion_date", DateType(),      True),
])

_issues_schema = StructType([
    StructField("id",             LongType(),      True),
    StructField("project_id",     LongType(),      True),
    StructField("title",          StringType(),    True),
    StructField("description",    StringType(),    True),
    StructField("reporter_id",    LongType(),      True),
    StructField("assignee_id",    LongType(),      True),
    StructField("status",         StringType(),    True),
    StructField("severity",       StringType(),    True),
    StructField("created_at",     TimestampType(), True),
    StructField("ingestion_date", DateType(),      True),
])

_comments_schema = StructType([
    StructField("id",             LongType(),      True),
    StructField("project_id",     LongType(),      True),
    StructField("author_id",      LongType(),      True),
    StructField("entity_type",    StringType(),    True),
    StructField("entity_id",      LongType(),      True),
    StructField("body",           StringType(),    True),
    StructField("created_at",     TimestampType(), True),
    StructField("ingestion_date", DateType(),      True),
])
