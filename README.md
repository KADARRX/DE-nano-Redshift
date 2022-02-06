# Data warehousing with Amazon Redshift for Sparkify

## About
#### Sparkify is a music app which is similar to Spotify, which is used to stream songs. The purpose of the project is to design a data model and process the data using Postgres database. The input datasets are available in json format. The data processing involves parsing the json formatted song and log datasets, extract the data and load them into the Postgres database.

### Contents of the project
- dwh.cfg
- sqlqueries.py
- create_tables.py
- etl.py


The sparkify data modelling has 1 fact table and 4 dimension tables created from two types of json files, songdata and logdata

The file `sqlqueries.py` includes the `CREATE`, `DROP`, `INSERT` statements needed to create and load the 2 staging tables and 5 main tables above into the redshift database.

The script `create_tables.py` executes the drop and create functions using the statements from `sqlqueries.py`.

The script `etl.py` processes the songdata and log files by loading them from s3 path, extracts the data and loads them into the tables created in redshift db.

The script `sample_queries.sql` can be used to verify the successful load of the files into the tables created. The same queries can also be run by connecting to redshift cluster on aws and using the query editor.

### To run the pipeline:
* Create a redshift cluster and create an iam role with access to the cluster. Note doen the iam_role arn details.
* Edit ```dwh.cfg``` and enter the information in the corresponding sections (CLUSTER and IAM_ROLE)  
* At the terminal, execute 
    > `python create_tables.py`
* Verify that tables are created and select can be performed on the tables by running `test.ipynb`. Close the notebook.
* At the terminal, execute below to load data from s3 into staging tables and subsequently load them into target tables
    > `python etl.py`
* Verify that all the 5 main tables return the count of rows and sample data by running `sample_queries.sql` on redshift query editor.
