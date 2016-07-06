#!/bin/bash
#
#
# Licensed under the Apache Software Foundation (ASF) under one
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
#

set -x
/bin/pwd
BASE_DIR=.
TARGET_DIR=./target
DOWNLOAD_DIR=${BASE_DIR}/thirdparty
download() {
 url=$1;
 finalName=$TARGET_DIR/$2
 tarName=$(basename $url)
 sparkTar=${DOWNLOAD_DIR}/${tarName}
 rm -rf $BASE_DIR/$finalName
 if [[ ! -f $sparkTar ]];
  then
  curl -Sso $DOWNLOAD_DIR/$tarName $url
 fi
 tar -zxf $DOWNLOAD_DIR/$tarName -C $BASE_DIR
 mv $BASE_DIR/spark-1.3.1-bin-hadoop-2.5.0 $BASE_DIR/$finalName
}
mkdir -p $DOWNLOAD_DIR
download "https://s3-us-west-2.amazonaws.com/streamsets-public/thirdparty/spark-1.3.1-bin-hadoop-2.5.0.tar.gz" "spark"
