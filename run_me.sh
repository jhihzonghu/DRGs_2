#! /bin/bash
export _JAVA_OPTIONS=-Xmx1024M
time /opt/hadoop/sbin/start-all.sh
time /opt/hadoop/bin/hadoop dfsadmin -safemode leave
time /opt/hadoop/bin/hadoop dfs -put /opt/hadoop/input2/EHC_1st_round.log / &
time /opt/hadoop/bin/hadoop jar /opt/hadoop/bin/Untitled.jar
cd /opt/hadoop/output/
time sort -n -r -k2   part-r-00000 -o output.txt
time head -n 20 /opt/hadoop/output/output.txt

