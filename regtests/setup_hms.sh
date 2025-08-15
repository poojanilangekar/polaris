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

# Download Hadoop distro. We technically don't use this for the test but Hive requires HADOOP_HOME to be set.
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

# Kill any running metastore server. 
echo "Killing any running metastore server..."
lsof -i :9083 | grep LISTEN | awk '{print $2}' | xargs kill -9

if [ "$1" != "clean_metastore" ]; then 
    echo "Using existing metastore database..."
else
    echo "Deleting metastore database and warehouse..."
    rm -rf /tmp/data/

    echo "Initializing metastore schema..."
    # Initialize the metastore. 
    $HIVE_HOME/bin/schematool -initSchema -dbType derby --verbose
fi 

echo "Starting metastore server..."
# Start the metastore server in the background and redirect the output to /tmp/metastore.log 
$HIVE_HOME/bin/hive --skiphadoopversion --skiphbasecp --service metastore > /tmp/metastore.log 2>&1 &