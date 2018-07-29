---
layout: post
title: Airflow ETL for moving data from Postgres to Postgres
date:   2018-07-29 13:26:00 -0400
tags: airflow etl postgresql postgres
categories: data-engineering etl
comments: true
---

## The problem

You need to move data from a PostgreSQL table to another. It could be a table from the same or a different database.

## Solution 1

The easiest way would be to use the [psycopg](http://initd.org/psycopg/) library to grab the data from the source using a normal cursor and then
intert it into the destination table:

{% highlight python %}
src = PostgresHook(postgres_conn_id='source', schema='source_schema')
dest = PostgresHook(postgres_conn_id='dest', schema='dest_schema')

src_conn = src.get_conn()
cursor = src_conn.cursor()
cursor.execute("SELECT * FROM users;")
dest.insert_rows(table=MY_DEST_TABLE, rows=cursor)
{% endhighlight %}

This would work fine for small amounts of data. But it wouldn't be performant if the size of the table is too big. The Airflow worker
would load all the data retrieved from the query into memory before loading it into the destination table, making it difficult if you are
copying GB's or even TB's of data. For that case, we can use a [server side cursor](http://initd.org/psycopg/docs/usage.html#server-side-cursors) instead.
This type of cursor doesn't fetch all the rows at once, but instead it uses batches.

For example, let's say you want to grab all the users created on a certain day and copy them to another table on a daily basis.
You could use this simple PythonOperator:

{% highlight python %}
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from psycopg2.extras import execute_values

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 4, 29),
    'email': ['email@mail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('postgres_to_postgres', default_args=default_args,
	schedule_interval='@daily')

def etl(ds, **kwargs):
    execution_date = kwargs['execution_date'].strftime('%Y-%m-%d')
    query = """
SELECT *
FROM users
WHERE created_at::date = date '{}'
    """.format(execution_date)

    src_conn = PostgresHook(postgres_conn_id='source',
                            schema='source_schema').get_conn()
    dest_conn = PostgresHook(postgres_conn_id='dest',
                             schema='dest_schema').get_conn()

    # notice this time we are naming the cursor for the origin table
    src_cursor = src_conn.cursor("serverCursor")
    src_cursor.execute(query)
    dest_cursor = dest_conn.cursor()

    # iterate over the cursor to get the records in batches
    while True:
        records = src_cursor.fetchmany(size=2000)
        if not records:
            break
        execute_values(dest_cursor,
                       "INSERT INTO users VALUES %s",
                       records)
        dest_conn.commit()

    src_cursor.close()
    dest_cursor.close()
    src_conn.close()
    dest_conn.close()

t1 = PythonOperator(
    task_id='etl',
    provide_context=True,
    python_callable=etl,
    dag=dag)
{% endhighlight %}

As you can see, we are grabbing the users that were created on the execution date. This gives us more control when running backfills.
Also, notice that since we're using a server side cursor, we have to iterate until the cursor doesn't contain any more rows. I'm
using a batch size of 2000 as an example.
Finally, psycopg2 provides a nice method called `execute_values` that allows us to insert the whole batch at once.

Keep in mind that if you have several Dags and tasks doing the same thing, it's probably more convenient to encapsulate the logic and write a custom Operator. On the other hand,
the transformations in between the loads could vary a lot, so it can become a bit trickier to reuse the code. It's up to you to decide.

Thanks for reading!
