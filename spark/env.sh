#!/bin/bash

PATH=$PATH:$HOME/.local/bin:$HOME/bin

export JAVA_HOME=/local/JAVA
export PATH=$JAVA_HOME/bin:$PATH:$HOME/bin

export SPARK_HOME=/local/SPARK
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

export PATH=/local/JAVA/bin:$PATH

export HADOOP_HOME=/local/HADOOP
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
