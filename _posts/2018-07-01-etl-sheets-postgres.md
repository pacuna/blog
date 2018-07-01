---
layout: post
title: Airflow ETL for Google Sheets and PostgreSQL
date:   2018-07-01 15:58:41 -0400
tags: airflow etl postgresql postgres data drive
categories: airflow etl data-engineering
---

## The problem

Imagine you have a table with sales that looks like this:

![Sales table]({{ "/assets/etl-drive-postgres/sales_table.png" | absolute_url }})

Now, image that those sales still have to be closed by a sales person after they enter this table.

One typical problem that can arise from this process, is that the status of the sale may be managed in a different source. For example an Excel sheet or in the case for this tutorial, on a Google spreadsheet.

The spreadsheet could be something like:

![Sales spreadsheet]({{ "/assets/etl-drive-postgres/spreadsheet.png" | absolute_url }})

This way, every time a sale enters the database, maybe the sales team sees this new sale on some internal dashboard and adds it to this spreadsheet. Then, they start making contact with the client and update the status accordingly.

This type of data integration is a very common problem inside companies. Luckily, we have several tools we could use to tackle this issue in an efficient and clean manner. This means, easy to monitor, easy to maintain and readable.

## The solution

There are several technologies out there that can help you solve this problem. In my case I chose [Airflow](https://airflow.apache.org/). So I'll show you how to write a simple Airflow DAG to extract the data from the csv file, filter the columns we need, and send it to a Postgres database.

### Google Drive access

In order to access the Google Drive API, we need to download a credentials file. You can follow the steps mentioned in the [official documentation](https://developers.google.com/drive/api/v3/quickstart/python). The process is pretty straightforward and you should end up with a `client_secret.json` file which is later used to authorize the first request. After that, another `credentials.json` file will be automatically downloaded and all the subsequent requests will use this file instead.

Let's create a basic script to download our sheet as a csv file and to authorize the application. We can do that with the following snippet:

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

A couple of dependencies you'll need to run this script: `google-api-python-client` and `oauth2client`.

Don't lose the `credentials.json` file, you'll need it later so Airflow can access the API without have to go through the browser authorization process.

### Target table

Let's create a table to hold the data we'll extract from the spreadsheet:

{% highlight sql %}
create table if not exists sale_statuses
(
	sale_id integer,
	status varchar(255),
	updated_at timestamp
)
;

create index if not exists sale_statuses_sale_id_index
	on sale_statuses (sale_id)
;

{% endhighlight %}

Later you can cross the information from this table with your original sales table to have all the information.

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

We can use pretty much keep all the default configuration from the tutorial example, but let's adjust the start date to yesterday, so
the DAG runs only once when testing it:

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
dag = DAG("update_sales", default_args=default_args, schedule_interval='@daily')
{% endhighlight %}

Make sure you adjust your start_date and schedule_interval to suit your needs. In this case, if I turn on the DAG, it will run once for every day since the start date until now.

The first operator will by a `PythonOperator`. It will use a function to download the spreadsheet as a csv file. We'll use the same code we used to authorize the application the first time, but now we have the `credentials.json` file so we don't need to authorize again via the web browser. I created a `sales` folder in the dags directory and moved both the `client_secret` and `credentials` json files into it. The DAG script also has to be in that folder.

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

This method will download the information and write a `statuses.csv` file in the same folder.

We are going to need two more python functions for other operators. One to select the columns we need, which in this case are `sale_id` and `status` and another one to load the resulting csv file into the target table:

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

# Load the final csv file to the database. Replace with your database credentials.
def load_to_dwh(ds, **kwargs):
    conn = pg.connect("host=PG_HOST dbname=PG_NAME user=PG_USER password=PG_PASSWORD")
    cur = conn.cursor()
    f = open(r'/home/pacuna/airflow/dags/sales/final.csv', 'r')
    f.seek(0)
    cur.copy_from(f, 'sale_statuses', sep=',')
    conn.commit()
    f.close()

{% endhighlight %}

Finally, let's create another task to truncate the database. Since this spreadsheet could be randomly updated, it's easy to just truncate the table and reload the entire dataset. For that, let's do something different and take advantage of the `PostgresOperator`.

First, add a connection for a new Postgres database using the Airflow connections menu:

![Airflow connection]({{ "/assets/etl-drive-postgres/airflow_connection.png" | absolute_url }})

Remember the connection id for later use.

Now we can define the tasks using the Airflow operators:

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

Since these tasks have dependencies between them, we need to declare the order in which they'll run:

{% highlight python %}
t2.set_upstream(t1)
t3.set_upstream(t2)
t4.set_upstream(t3)
{% endhighlight %}

The complete script looks like this:

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

dag = DAG("update_sale_statuses", default_args=default_args, schedule_interval='@daily')


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

Now, if you go to the Airflow dashboard, you should see your Dag in the list.

![Airflow dashboard]({{ "/assets/etl-drive-postgres/airflow_dashboard.png" | absolute_url }})

You can toggle the off button of the Dag to start it, and hit refresh a couple of times until it finishes:

![Airflow finished]({{ "/assets/etl-drive-postgres/airflow_finished.png" | absolute_url }})

As you can see the four tasks were run successfully, and since my start date was yesterday, it only ran once and will keep running once a day. If you go to your sales dags directory, you should see both the statuses and the final csv file:

```sh
$ ls
client_secret.json  credentials.json  final.csv  __pycache__  sales_etl.py  statuses.csv
```

These files will be overwritten every time the dag runs.

And if you check your `sale_statuses` table, you should see the data that has been loaded:

![Statuses table]({{ "/assets/etl-drive-postgres/statuses_table.png" | absolute_url }})

And that's it! I'm still learning a lot about Airflow and ETL's in general so if you have any comments or suggestion, you can leave a comment below.

Thanks for reading!
