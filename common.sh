#!/bin/bash

AGENT_HOME=`dirname $0`

for jar in $AGENT_HOME/lib/hector-core-1.0-3/*.jar; do
    CLASSPATH=$CLASSPATH:$jar
done

for jar in $AGENT_HOME/lib/apache-commons/*.jar; do
    CLASSPATH=$CLASSPATH:$jar
done

for jar in $AGENT_HOME/lib/jackson-2.0.0/*.jar; do
    CLASSPATH=$CLASSPATH:$jar
done

for jar in $AGENT_HOME/lib/jdbc/*.jar; do
    CLASSPATH=$CLASSPATH:$jar
done

if [ -d "$AGENT_HOME/build" ]; then
    CLASSPATH=$CLASSPATH:$AGENT_HOME/build
else
	CLASSPATH=$CLASSPATH:$AGENT_HOME/prebuild
fi
