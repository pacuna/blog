---
layout: post
title: Airflow ETL for Google Sheets and PostgreSQL
date:   2018-07-01 15:58:41 -0400
tags: airflow etl postgresql postgres data drive
categories: data-engineering etl
comments: true
---

This is going to be the first of a series of posts related to [Apache Airflow](https://airflow.apache.org/).
I've been writing and migrating a couple of small ETL jobs at work to Airflow and some of this information might be useful to someone facing similar problems.

## Description

In this first post, I'll discuss an approach to extract data from a Google Drive Spreadsheet and load it into a PostgreSQL database.

For my specific requirement, an incremental update wasn't needed. So using a simple truncate table action before inserting the latest data was enough.

## Scenario

Imagine you have a table with sales that looks like this:

![Sales table]({{ "/assets/etl-drive-postgres/sales_table.png" | absolute_url }})

You can see there's the primary key, a product id, the total amount of the sale and a timestamp.

Now imagine those sales still had to be closed by a sales person after they enter this table.
One typical problem that can arise from this process, is that the status of the sale may be managed on a different data source. 
Typically, companies will use some sort of CRM, but in some cases they might want to use something lighter if the operation isn't big enough yet.
This could be and Excel sheet for example, or in the case for this tutorial, a Google Drive Spreadsheet.

The spreadsheet might look something like this:

![Sales spreadsheet]({{ "/assets/etl-drive-postgres/spreadsheet.png" | absolute_url }})

Here we have the sale's id associated to the sale's entry in the database, a status and some extra data we don't care about here.
This way, every time a sale enters the database, maybe the sales team sees it on some internal dashboard and adds it to this spreadsheet, or maybe some trigger function adds it
automatically. Then, they reach out the client and update the status accordingly.

Even if it isn't for something as important as a sale, the use of cloud-based documents it's increasing over time, and data integration tools support for them is becoming essential.

## Using Python and Airflow

The cool thing about tools like Airflow, is than even though they work in a lower level (lower than typical Data Integration tools), they are much more flexible when you need to write
integration code for other platforms. You just need support for Python and you're done. Whereas with other tools, you might need special libraries and drivers, and if they're not available, good luck writing your own.

For this particular case we'll write a simple Airflow DAG to extract the data from the Spreadsheet to a csv file, filter the columns we need, and load it into our PostgreSQL database so
we can join this new data with our original sales data for posterior reporting.

### Google Drive access

In order to access the Google Drive API, we need to download a credentials file. You can follow the steps mentioned in the [official documentation](https://developers.google.com/drive/api/v3/quickstart/python).
The process is pretty straightforward and you should end up with a `client_secret.json` file which is later used to authorize the first request.
After that, another `credentials.json` file will be automatically downloaded and all the subsequent requests will use this file instead.

Let's create a basic script to download our sheet as a csv file and to authorize the application. We can do that with the following code:

{% highlight python %}

# Load dependencies
from apiclient.discovery import build
from httplib2 import Http
from oauth2client import file, client, tools
from googleapiclient.http import MediaIoBaseDownload
import io

# Authorization: first time you run this, a new browser window or tab will 
# be opened in order to authorize the application. Also,
# the credentials.json file will be downloaded.
SCOPES = 'https://www.googleapis.com/auth/drive'
store = file.Storage('credentials.json')
creds = store.get()
if not creds or creds.invalid:
    flow = client.flow_from_clientsecrets('client_secret.json', SCOPES)
    creds = tools.run_flow(flow, store)
service = build('drive', 'v3', http=creds.authorize(Http()))

# Replace this file_id with your spreadsheet file id 
file_id = '1kYN65lVCk7Zu3OC-tocJ9Xm8Iyq73hT56iyXCGLz-DU'
request = service.files().export_media(fileId=file_id, mimeType='text/csv')

# The file will be saved as sales.csv in the same directory
fh = io.FileIO('sales.csv', 'wb')
downloader = MediaIoBaseDownload(fh, request)
done = False
while done is False:
    status, done = downloader.next_chunk()
    print("Download %d%%." % int(status.progress() * 100))
{% endhighlight %}

Some dependencies you'll need to run this script are: `google-api-python-client` and `oauth2client`.

Don't lose the `credentials.json` file, you'll need it later so Airflow can access the API without have to go through the browser authorization process.

### Target table

Let's create a table to hold the data we'll extract from the spreadsheet:

{% highlight sql %}
create table if not exists sale_statuses
(
	sale_id integer,
	status text,
	updated_at timestamp
)
;

create index if not exists sale_statuses_sale_id_index
	on sale_statuses (sale_id)
;

{% endhighlight %}

### Airflow

We are ready to write our Airflow DAG. If you are not familiarized with the Airflow concepts, check out the [tutorial](https://airflow.apache.org/tutorial.html) for a quick pipeline definition.

First, we need to import some dependencies:

{% highlight python %}
# airflow stuff
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime, timedelta

# for postgres access
import psycopg2 as pg

# for google auth and csv manipulation
from apiclient.discovery import build
from httplib2 import Http
from oauth2client import file, client, tools
from googleapiclient.http import MediaIoBaseDownload
import io
import csv
import pandas as pd
{% endhighlight %}

We can keep pretty much all the default configurations from the tutorial example. Some things we need to change though,
are the catchup parameter, so we can choose any start date and the backfill will not run for the past dates. In this case, it doesn't make sense
to run a backfill, because there's only one state for each entry in the spreadsheet. If you run the dag for yesterday let's say, you would have the same data that you got for today.
Also, make sure you don't run this task concurrently because it's not idempotent.
This means that concurrent runs can alter the state of the other running tasks and the database could end up
in an inconsistent state. For example, if you have 2 dags running simultaneously, one may truncate the table just before the other dag inserts the data, leaving
the data fully duplicated.

It's a good practice to write idempotent dags with Airflow, but for this specific case it doesn't have too much sense to worry about it, since these tasks don't require backfills nor
concurrent runs. Anyway, just to be safe, you could also configure the concurrency of the dag by using the `max_active_runs` and the `concurrency` parameters.

{% highlight python %}
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2018, 6, 30),
    "email": ["pabloacuna88@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

# use a daily interval for the schedule
dag = DAG("update_sales", default_args=default_args, schedule_interval='@daily', catchup=False)
{% endhighlight %}

Make sure you adjust the schedule_interval for your case. In this case, if I turn on the DAG, it will run once each day starting from today.

The first operator will be a `PythonOperator`. It will use a python method to download the spreadsheet as a csv file.
We'll use the same code we used to authorize the application the first time, 
but now we have the `credentials.json` file so we don't need to authorize again via the web browser.

I created a `sales` folder in the dags directory and moved both the `client_secret` and `credentials` json files into it. The DAG script has to be in that same folder.
Make sure to change the paths for the files being used and created.

{% highlight python %}
def download_csv(ds, **kwargs):

    # Setup the Drive v3 API, change the paths accordingly 
    SCOPES = 'https://www.googleapis.com/auth/drive'
    store = file.Storage('/home/pacuna/airflow/dags/sales/credentials.json')
    creds = store.get()
    if not creds or creds.invalid:
        flow = client.flow_from_clientsecrets('/home/pacuna/airflow/dags/sales/client_secret.json', SCOPES)
        creds = tools.run_flow(flow, store)
    service = build('drive', 'v3', http=creds.authorize(Http()))

    # use your own spreadsheet id
    file_id = '1YNQD7JZGxkFvPbPv3C9wixPbyGXAPedJQH9MMl7UQ6k'
    request = service.files().export_media(fileId=file_id,
                                                 mimeType='text/csv')
    fh = io.FileIO('/home/pacuna/airflow/dags/sales/statuses.csv', 'wb')
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
        print("Download %d%%." % int(status.progress() * 100))

{% endhighlight %}

This method will download the spreadsheet as a `statuses.csv` file and will put it in the same folder.

We are also going to need two more python functions for other operators.
One to select the columns we need, which in this case are `sale_id` and `status`, and another one to load the resulting csv file into the target table:

{% highlight python %}

# Use pandas to manipulate the csv generated by the previous function
def select_columns(ds, **kwargs):
    df = pd.read_csv("/home/pacuna/airflow/dags/sales/statuses.csv")

    # in case we have empty rows
    filtered_df = df[df['sale_id'].notnull()]

    # we need to send the timestamp row too
    filtered_df['updated_at'] = 'now()'

    # export the final file
    filtered_df[['sale_id', 'status', 'updated_at']].to_csv('/home/pacuna/airflow/dags/sales/final.csv', index=False, header=False)

# Load the final csv file into the database. Replace with your database credentials.
def load_to_dwh(ds, **kwargs):
    conn = pg.connect("host=PG_HOST dbname=PG_NAME user=PG_USER password=PG_PASSWORD")
    cur = conn.cursor()
    f = open(r'/home/pacuna/airflow/dags/sales/final.csv', 'r')
    f.seek(0)
    cur.copy_from(f, 'sale_statuses', sep=',')
    conn.commit()
    f.close()

{% endhighlight %}

Finally, let's create another task to truncate the table. For that, let's do something different and take advantage of the `PostgresOperator`.

First, add a connection for a new PostgreSQL database using the Airflow connections menu:

![Airflow connection]({{ "/assets/etl-drive-postgres/airflow_connection.png" | absolute_url }})

Remember the connection id for later use.

Now we can define the tasks using the Python and Postgres operators:

{% highlight python %}
t1 = PythonOperator(
    task_id='download_csv_from_drive',
    provide_context=True,
    python_callable=download_csv,
    dag=dag)

t2 = PythonOperator(
    task_id='select_columns',
    provide_context=True,
    python_callable=select_columns,
    dag=dag)

# replace with your connection data
t3 = PostgresOperator(
    task_id='truncate_statuses_table',
    postgres_conn_id="dwh",
    sql="TRUNCATE table sale_statuses",
    database="postgres",
    dag=dag)

t4 = PythonOperator(
    task_id='load_to_dwh',
    provide_context=True,
    python_callable=load_to_dwh,
    dag=dag)
{% endhighlight %}

Since these tasks depend on each other, we need to declare the order in which they'll run:

{% highlight python %}
t2.set_upstream(t1)
t3.set_upstream(t2)
t4.set_upstream(t3)
{% endhighlight %}

The final script should look like this:

{% highlight python %}
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime, timedelta
import psycopg2 as pg
from apiclient.discovery import build
from httplib2 import Http
from oauth2client import file, client, tools
from googleapiclient.http import MediaIoBaseDownload
import io
import csv
import pandas as pd


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2018, 6, 30),
    "email": ["pabloacuna88@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG("update_sale_statuses", default_args=default_args, schedule_interval='@daily', catchup=False)


def download_csv(ds, **kwargs):

    # Setup the Drive v3 API
    SCOPES = 'https://www.googleapis.com/auth/drive'
    store = file.Storage('/home/pacuna/airflow/dags/sales/credentials.json')
    creds = store.get()
    if not creds or creds.invalid:
        flow = client.flow_from_clientsecrets('/home/pacuna/airflow/dags/sales/client_secret.json', SCOPES)
        creds = tools.run_flow(flow, store)
    service = build('drive', 'v3', http=creds.authorize(Http()))

    file_id = '1kYN65lVCk7Zu3OC-tocJ9Xm8Iyq73hT56iyXCGLz-DU'
    request = service.files().export_media(fileId=file_id,
                                                 mimeType='text/csv')
    fh = io.FileIO('/home/pacuna/airflow/dags/sales/statuses.csv', 'wb')
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
        print("Download %d%%." % int(status.progress() * 100))

def select_columns(ds, **kwargs):
    df = pd.read_csv("/home/pacuna/airflow/dags/sales/statuses.csv")
    filtered_df = df[df['sale_id'].notnull()]
    filtered_df['updated_at'] = 'now()'
    filtered_df[['sale_id', 'status', 'updated_at']].to_csv('/home/pacuna/airflow/dags/sales/final.csv', index=False, header=False)

def load_to_dwh(ds, **kwargs):
    conn = pg.connect("host=localhost dbname=postgres user=postgres password=mysecretpassword")
    cur = conn.cursor()
    f = open(r'/home/pacuna/airflow/dags/sales/final.csv', 'r')
    f.seek(0)
    cur.copy_from(f, 'sale_statuses', sep=',')
    conn.commit()
    f.close()

t1 = PythonOperator(
    task_id='extract_from_drive',
    provide_context=True,
    python_callable=download_csv,
    dag=dag)

t2 = PythonOperator(
    task_id='select_columns',
    provide_context=True,
    python_callable=select_columns,
    dag=dag)

t3 = PostgresOperator(
    task_id='truncate_dwh_table',
    postgres_conn_id="dwh",
    sql="TRUNCATE table sale_statuses",
    database="postgres",
    dag=dag)

t4 = PythonOperator(
    task_id='load_to_dwh',
    provide_context=True,
    python_callable=load_to_dwh,
    dag=dag)

t2.set_upstream(t1)
t3.set_upstream(t2)
t4.set_upstream(t3)

{% endhighlight %}

Now, if you go to the Airflow dashboard, you should see your Dag on the list. If not, try restarting your server and scheduler.

![Airflow dashboard]({{ "/assets/etl-drive-postgres/airflow_dashboard.png" | absolute_url }})

You can toggle the off button of the Dag to start it, and hit refresh a couple of times until it finishes:

![Airflow finished]({{ "/assets/etl-drive-postgres/airflow_finished.png" | absolute_url }})

If you go to your sales dags directory, you should see both the statuses and the final csv file:

```sh
$ ls
client_secret.json  credentials.json  final.csv  __pycache__  sales_etl.py  statuses.csv
```

These files will be overwritten every time the dag runs.

And if you check your `sale_statuses` table, you should see the data that has been loaded:

![Statuses table]({{ "/assets/etl-drive-postgres/statuses_table.png" | absolute_url }})

Now you can do all sorts of crazy joins and create beautiful reports for your boss.

And that's it! I'm still learning a lot about Airflow and ETL's in general so if you have any comments or suggestions, you can leave a message below.

Thanks for reading!
