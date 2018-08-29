---
layout: post
title: Airflow ETL for moving data from Postgres to Postgres
date:   2018-07-29 13:26:00 -0400
tags: airflow etl postgresql postgres
categories: data-engineering etl
comments: true
---

This is the third post from the Airflow series. Here we're covering a very common scenario: moving data from a table (or database) to another.
In this case we are working with two PostgreSQL databases, and we need an incremental update approach. Let's
see how to do that using Python and Airflow.

A quick way would be to use the [psycopg](http://initd.org/psycopg/) library to grab the data from the source using a regular *client side* cursor and then
insert it into the destination table. We can define both connections in Airflow by using the `PostgresHook` class. This hook only needs the
`connection_id` parameter which you can get by creating a connection using the Airflow UI in the connections menu.

{% highlight python %}
src = PostgresHook(postgres_conn_id='source', schema='source_schema')
dest = PostgresHook(postgres_conn_id='dest', schema='dest_schema')

src_conn = src.get_conn()
cursor = src_conn.cursor()
cursor.execute("SELECT * FROM users;")
dest.insert_rows(table=MY_DEST_TABLE, rows=cursor)
{% endhighlight %}

As you can see, we start the connections, get a cursor for the source table to get the data, and then insert it into the destination table. Pretty simple.

This would work fine for a small or medium amount of data. But it wouldn't be performant if the size of the table is larger.
The Airflow worker would have to load all the data retrieved from the query into memory before inserting it into the destination table, making it difficult if you are
copying GB's or even TB's of data. For this case, we would do better using a [server side cursor](http://initd.org/psycopg/docs/usage.html#server-side-cursors) instead.
This type of cursor doesn't fetch all the rows at once, but instead it uses batches. This way you can control the amount of data that's transfered to the client.

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
    'retries': 1,
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
    # that's going to force the library to create a server cursor
    src_cursor = src_conn.cursor("serverCursor")
    src_cursor.execute(query)
    dest_cursor = dest_conn.cursor()

    # now we need to iterate over the cursor to get the records in batches
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

As you can see, we are grabbing the users that were created on the execution date. This gives us more control when running backfills since
we could parallelize the execution for each different day we need to copy.
Also, notice that since we're using a server side cursor, we have to iterate until the cursor is empty. I'm
using a batch size of 2000 as an example.

Finally, psycopg2 provides a nice method called `execute_values` that allows us to insert the whole batch at once.

Keep in mind that if you have several Dags and tasks using the same code, it's more convenient to encapsulate the logic and write a custom Operator. On the other hand,
the transformations in between the extract and load stages could vary a lot, so it can become a bit tricky to reuse the code. It's up to you to decide. 

If you are building an ETL that uses a different database engine, you should check if it has a similar feature, in which you can push the work onto the database server
instead your worker machines. A good rule of thumb is that the Airflow workers should never be in charge of doing heavy work.

Thanks for reading!
