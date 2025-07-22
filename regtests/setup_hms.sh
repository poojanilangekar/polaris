#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# Idempotent setup for hive metasore tests. Downloads hive and 
# hadoop distributions  and sets up hive-site.xml. 
# Unless HIVE_VERSION is set, defaults to 3.1.3.
# Unless HADOOP_VERSION is set, defaults to 3.2.0.
#
# Warning - first time setup may download large amounts of files
# Warning - may modify hive-site.xml

set -x 

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

# Download HIVE distro. 
if [ -z "${HIVE_VERSION}" ]; then 
    export HIVE_VERSION="hive-3.1.3"
fi
export HIVE_DISTRIBUTION="apache-${HIVE_VERSION}-bin"

export HIVE_HOME=$(realpath ~/${HIVE_DISTRIBUTION})

if [ -z "${HIVE_HOME}" ]; then 
    if ! [ -f ~/${HIVE_DISTRIBUTION}.tgz ]; then
        echo "Downloading hive distro..."
        wget -O ~/${HIVE_DISTRIBUTION}.tgz "https://archive.apache.org/dist/hive/${HIVE_VERSION}/${HIVE_DISTRIBUTION}.tar.gz"
        if ! [ -f ~/${HIVE_DISTRIBUTION}.tgz ]; then
            echo "Failed to download hive distro. Please check the logs."
            exit 1
        fi
    else 
        echo "Found existing hive distro tarball"
    fi

    tar xzvf ~/${HIVE_DISTRIBUTION}.tgz -C ~
    if [ $? -ne 0 ]; then
        echo "Failed to extract hive distro. Please check the logs."
        exit 1
    else 
        echo "Extracted hive distro."
        export HIVE_HOME=$(realpath ~/${HIVE_DISTRIBUTION})
        rm ~/${HIVE_DISTRIBUTION}.tgz
    fi
fi 

echo "Hive distro at ${HIVE_HOME}"

# Download Hadoop distro. 
if [ -z "${HADOOP_VERSION}" ]; then 
    export HADOOP_VERSION="3.3.6"
fi 
export HADOOP_DISTRIBUTION="hadoop-${HADOOP_VERSION}"

export HADOOP_HOME=$(realpath ~/${HADOOP_DISTRIBUTION})

if [ -z "${HADOOP_HOME}" ]; then 
    if ! [ -f ~/${HADOOP_DISTRIBUTION}.tar.gz ]; then
        echo "Downloading hadoop distro..."
        # Unlike Spark and Hive, Hadoop distros live at downloads.apache.org
        wget -O ~/${HADOOP_DISTRIBUTION}.tar.gz "https://downloads.apache.org/hadoop/common/${HADOOP_DISTRIBUTION}/${HADOOP_DISTRIBUTION}.tar.gz"
        if ! [ -f ~/${HADOOP_DISTRIBUTION}.tar.gz ]; then
            echo "Failed to download hadoop distro. Please check the logs."
            exit 1
        fi
    else 
        echo "Found existing hadoop distro tarball"
    fi

    tar xzvf ~/${HADOOP_DISTRIBUTION}.tar.gz -C ~
    if [ $? -ne 0 ]; then
        echo "Failed to extract hadoop distro. Please check the logs."
        exit 1
    else 
        echo "Extracted hadoop distro."
        export HADOOP_HOME=$(realpath ~/${HADOOP_DISTRIBUTION})
        rm ~/${HADOOP_DISTRIBUTION}.tar.gz
    fi
fi 


# Update the PATH to include HADOOP_HOME and HIVE_HOME. 
export PATH=${HIVE_HOME}/bin:${HADOOP_HOME}/bin:$PATH
HIVE_CONF_DIR="${HIVE_HOME}/conf"

touch "${HIVE_CONF_DIR}/hive-site.xml"
touch "${HIVE_CONF_DIR}/hive-log4j2.properties"

# Populate the hive-site.xml file. 
cat <<EOF > "${HIVE_CONF_DIR}/hive-site.xml"
<?xml version="1.0" encoding="UTF-8"?>
<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->
<configuration>
    <property>
        <name>hive.server2.enable.doAs</name>
        <value>false</value>
    </property>
    <property>
        <name>hive.exec.submit.local.task.via.child</name>
        <value>false</value>
    </property>
    <property>
        <name>hive.compactor.worker.threads</name>
        <value>1</value>
    </property>
    <property>
        <name>mapreduce.framework.name</name>
        <value>local</value>
    </property>
    <property>
        <name>javax.jdo.option.ConnectionURL</name>
        <value>jdbc:derby:;databaseName=/tmp/data/hms/metastore_db;;create=true</value>
    </property>
    <property>
        <name>metastore.metastore.event.db.notification.api.auth</name>
        <value>false</value>
    </property>
</configuration>
EOF


# # Download the iceberg runtime JAR to the Spark classpath and set the 
# # Spark catalog implementation to use the hive metastore. 
if [ -z "${SPARK_VERSION}" ]; then 
    echo "SPARK_VERSION is not set. Please set it to the path to the Spark distribution."
    exit 1
else
    SPARK_HOME=$(realpath ~/spark-${SPARK_VERSION}-bin-hadoop3)
    if [ -z "${SPARK_HOME}" ]; then 
        echo "Spark distribution not found. Please use setup.sh script to download it."
        exit 1
    fi

    ICEBERG_VERSION="1.9.0"
    SCALA_VERSION="2.12"
    SPARK_MAJOR_VERSION="$(echo ${SPARK_VERSION} | cut -d "." -f 1).$(echo ${SPARK_VERSION} | cut -d "." -f 2)"

    ICEBERG_RUNTIME_JAR="${SPARK_HOME}/jars/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_${SCALA_VERSION}-${ICEBERG_VERSION}.jar"
    if ! [ -f ${ICEBERG_RUNTIME_JAR} ]; then 
        echo "Downloading iceberg runtime JAR..."
        ICEBERG_RUNTIME_JAR_URL="https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_${SCALA_VERSION}/${ICEBERG_VERSION}/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_${SCALA_VERSION}-${ICEBERG_VERSION}.jar"
        wget -O ${ICEBERG_RUNTIME_JAR} ${ICEBERG_RUNTIME_JAR_URL}
        if [ $? -ne 0 ]; then 
            echo "Failed to download iceberg runtime JAR. Please check the logs."
            exit 1
        fi
    else 
        echo "Found existing iceberg runtime JAR"
    fi
fi


SPARK_CONF="${SPARK_HOME}/conf/spark-defaults.conf"
echo "Vefitying Spark conf..."
if grep 'HIVE_METASTORE_ICEBERG_TESTCONF' ${SPARK_CONF} 2>/dev/null; then
    echo "Hive metastore iceberg conf already set"
else 
    echo "Setting hive metastore iceberg conf..."
    # Instead of clobbering existing spark conf, just comment it all out in case it was customized carefully.
    sed -i 's/^/# /' ${SPARK_CONF}
cat << EOF >> ${SPARK_CONF}

# HIVE_METASTORE_ICEBERG_TESTCONF
spark.sql.variable.substitute true
spark.driver.extraJavaOptions -Dderby.system.home=/tmp/data/spark-warehouse/

spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.spark_catalog.type=hive
spark.sql.catalog.spark_catalog.uri=thrift://localhost:9083
spark.sql.defaultCatalog=spark_catalog
spark.sql.warehouse.dir=/tmp/data/spark-warehouse
EOF
    echo "Success!"
fi


# Kill any running metastore server. 
echo "Killing any running metastore server..."
lsof -i :9083 | grep LISTEN | awk '{print $2}' | xargs kill -9

if [ "$1" != "clean_metastore" ]; then 
    echo "Using existing metastore database..."
else 
    echo "Clearing metastore database..."
    # Clear the metastore database. 
    rm -rf /tmp/data/hms

    echo "Initializing metastore schema..."
    # Initialize the metastore. 
    $HIVE_HOME/bin/schematool -initSchema -dbType derby --verbose
fi 

echo "Starting metastore server..."
# Start the metastore server in the background and redirect the output to /tmp/metastore.log 
$HIVE_HOME/bin/hive --skiphadoopversion --skiphbasecp --service metastore > /tmp/metastore.log 2>&1 &