import sys

SRC_PATH = "/Workspace/Users/manisha.tech.dey@outlook.com/PEI_PROJECT_2/src"

if SRC_PATH not in sys.path:
    sys.path.insert(0, SRC_PATH)

from pei_pipeline.main import main


from pei_pipeline.main import main

from validate_tables import validate_pipeline

result = main(

    spark=spark,

    dbutils=dbutils,

    stage="all"

)

if result["status"] != "SUCCESS":

    raise RuntimeError("Pipeline failed.")

validate_pipeline(

    spark

)

print("Smoke Test Passed.")