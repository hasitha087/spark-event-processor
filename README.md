Folder structure

truecaller-assesement
    |
    |-dataprocess
        |------ __init__.py
        |------configloader.py (python class which includes methods to load configuration parameters)
        |------dataprocessor.py (python class which includes data cleaning, remove duplicates and aggregation methods)
        |------dataprocessor_test.py (python class which includes unit tests for methods available in dataprocessor)
    |
    |-config
        |------config.ini (file which includes all the configuration parameters)
    |-event_data (input data in csv format)
    |    |-output (output data write to this folder using parwuet format)
    |-logs (inludes all the log files on daily basis)
    |-main.py (main python file)
    |-etl_dag.py (Airflow configuration file)
    |-README.md
    |-Data_Platform_Design_Proposal.pdf


This can be run using
spark-submit \
  --master <master-url> \
  --deploy-mode <deploy-mode> \
  --driver-memory <value>g \
  --executor-memory <value>g \
  --executor-cores <number of cores>  \
  main.py

Units tests can be run using
python -m unittest dataprocessor/dataprocessor_test.py