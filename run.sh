#!/bin/bash
if ! type spark-submit 2> /dev/null; then
	echo "You need to install Spark first!"
	exit 1
fi

spark-submit \
    --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=log4j.properties" \
    --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=log4j.properties" \
    --driver-class-path target/sparkml-flights-delay-1.0-SNAPSHOT-jar-with-dependencies.jar \
    --class fdiazgon.FlightsDelayApp target/sparkml-flights-delay-1.0-SNAPSHOT-jar-with-dependencies.jar \
    --explore raw/tuning.csv
