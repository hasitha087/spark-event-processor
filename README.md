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
