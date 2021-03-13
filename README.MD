# Shippy

Shippy is a Python library that implements Xomnia DE Assignment.

## Local Usage
From a terminal instance at root folder of the assignment, execute

```bash
python3 app.py
```
and go to the [web application](http://127.0.0.1:8080) in your browser. 

The main page displays all KPI calculations in HTML format. 

Hint: Each KPI description is linked to the REST API endpoint which serves the KPI in JSON format.

## Google Cloud Deployment
The application is deployed using Google App Engine standard python environment [here](https://xomnia-assignment.ew.r.appspot.com).

Please note that to keep the costs low, application is Disabled by default, and will be enabled temporarily for demonstration and review. Email to <ozantuna@hotmail.com> if the cloud deployment to be Enabled.
## Important Files
* __data_exploration.ipynb__: A Jupyter Notebook that demonstrates scratch implementations of data loading, cleaning and KPI calculation.
* __python_api_test.ipynb__: A Jupyter Notebook that implements interactive "unit tests" for Python API functionality.
* __ingest_pipeline.py__: Python application that ingests data files in specified folder (```data``` by default) folder into a sqlite database (```shippy/db/shippydb.sqlite``` by default).
* __app.py__: Flask application to serve the Python API as a REST API via web.