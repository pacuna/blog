---
layout: post
title: Airflow task to refresh PostgreSQL Materialized Views
date:   2018-07-05 21:15:00 -0400
tags: airflow postgresql materialized-views
categories: data-engineering airflow
comments: true
---

In this post we'll see how to refresh a PostgreSQL materialized view using an Airflow task.

In case you're not familiarized with materialized views, they're a special kind of view in which the data has been materialized. This means
the result of the query has been saved somewhere, making the retrieval much faster than a regular view.

Materialized views are static. You need to run a command to update the data to the latest version. As you may imagine, that makes them not very suitable
for transactional querying, but very useful to cache OLAP queries that don't need to be updated that often.

Since the update command it's just a SQL command, you could write a simple bash script to run it and then create a crontab entry to schedule the job.

The advantage of using something like Airflow here, is that you can easily monitor every run of the task. You can also stop it or starting it with just one click.
It might seem that using Airflow just to run a task like this one is overkilling, and I would agree if that were the case. But assuming you are already using Airflow
to orchestrate your task workflow, it would make sense to add every automated task to the same pool.

Now, in Airflow we have several operators which we can use for this task. Since we have a PostgreSQL database and we need to run a SQL command, let's just stick to the `PostgresOperator`. 
The advantage of this operator, is that once you have configured a database connection in Airflow, you only need its `connection_id` instead of authenticating against the database in your code every time.

The DAG can be something as simple as this:

{% highlight python %}
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2018, 7, 3),
    "email": ["youremail@mail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG("refresh_sales_matviews", default_args=default_args, schedule_interval='@daily', catchup=False)

# We want to refresh the sales view
# Replace with your connection id
t1 = PostgresOperator(
    task_id='refresh_sales_table',
    postgres_conn_id="my_conn_id",
    sql="refresh materialized view sales",
    database="my_db",
    dag=dag)
{% endhighlight %}

This will run a refresh command on a daily basis. Remember that in order to use the `PostgresOperator`, you first need to create a connection using the Airflow connections tab:

![Airflow connections]({{ "/assets/refresh-matviews/connections.png" | absolute_url }})

And then use that `connection_id` for the `postgres_conn_id` parameter. You can execute any SQL command by using this operator.

One thing to notice is that just as the previous Airflow post I wrote, we would never need to run backfills for this DAG, so the `catchup` parameter is set to `False`. 
We wouldn't also need to run concurrent tasks, since we only need the latest refresh. In this case the DAG is idempotent though. Concurrent runs wouldn't affect the data consistency, but it
also wouldn't make sense to do it. So be careful and always check the parameters that control the concurrency of the DAG and its tasks.

Thanks for reading!
