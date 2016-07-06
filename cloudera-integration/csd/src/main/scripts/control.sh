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

# For better debugging
date 1>&2

CMD=$1

function log {
  timestamp=$(date)
  echo "$timestamp: $1"       #stdout
  echo "$timestamp: $1" 1>&2; #stderr
}

function update_users {
  IFS=';' read -r -a array <<< "$CONFIGURED_USERS"
  for element in "${array[@]}"; do
    echo "$element" >> "$CONF_DIR"/"$FILE_AUTH_TYPE"-realm.properties
  done
  chmod 600 "$CONF_DIR"/"$FILE_AUTH_TYPE"-realm.properties
}

function generate_ldap_configs {
  ldap_configs=`cat "$CONF_DIR"/ldap.properties | grep "ldap" | grep -v "ldap.bindPassword" | sed -e 's/ldap\.\([^=]*\)=\(.*\)/  \1=\"\2\"/g'`
  echo "ldap {
  com.streamsets.datacollector.http.LdapLoginModule required
  bindPassword=\"@ldap-bind-password.txt@\"
  contextFactory=\"com.sun.jndi.ldap.LdapCtxFactory\"
$ldap_configs;
};" > "$CONF_DIR"/ldap-login.conf
  ldap_bind_password=`cat "$CONF_DIR"/ldap.properties | grep "ldap.bindPassword"`
  echo "$ldap_bind_password" | awk -F'=' '{ print $2 }' | tr -d '\n' > "$CONF_DIR"/ldap-bind-password.txt
}

# Create symlinks for standard hadoop services to SDC_RESOURCES directory
function create_config_symlinks {
  # Hadoop
  if [ ! -d $SDC_RESOURCES/hadoop-conf ]; then
    mkdir -p $SDC_RESOURCES/hadoop-conf
    ln -s /etc/hadoop/conf/*.xml $SDC_RESOURCES/hadoop-conf
  fi
  # Hbase
  if [ ! -d $SDC_RESOURCES/hbase-conf ]; then
    mkdir -p $SDC_RESOURCES/hbase-conf
    ln -s /etc/hbase/conf/*.xml $SDC_RESOURCES/hbase-conf
  fi
  # Hive
  if [ ! -d $SDC_RESOURCES/hive-conf ]; then
    mkdir -p $SDC_RESOURCES/hive-conf
    ln -s /etc/hive/conf/*.xml $SDC_RESOURCES/hive-conf
  fi
}

export SDC_CONF=$CONF_DIR

# Propagate system white and black lists
SDC_PROPERTIES=$SDC_CONF/sdc.properties
if ! grep -q "system.stagelibs.*list" $SDC_PROPERTIES; then
  echo "System white nor black list found in configuration"
  if [ -f $SDC_DIST/etc/sdc.properties ]; then
    echo "Propagating default white and black list from parcel"
    grep "system.stagelibs.*list" $SDC_DIST/etc/sdc.properties >> $SDC_PROPERTIES
  else
    echo "Parcel doesn't contain default configuration file, skipping white/black list propagation"
  fi
fi

case $CMD in

  (start)
    log "Starting StreamSets Data Collector"
    if [[ "$LOGIN_MODULE" = "file" ]]; then
      update_users
    else
      generate_ldap_configs
    fi

    create_config_symlinks

    source "$CONF_DIR"/sdc-env.sh
    exec $SDC_DIST/bin/streamsets dc -verbose -skipenvsourcing -exec

  (update_users)
    update_users
    exit 0

esac
