#Centralized Optimistic Concurrency Control @Shenglei Mao
### Things to notice
```
commit ac3d50fd381ed04aa835480d2455fc0b0c06298c (HEAD -> master)
Author: Shenglei-Mao <mao59@purdue.edu>
Date:   Fri May 1 21:20:04 2020 -0400

    4 sites working very stable now, but logging module is messed up

commit d93289b470c13e266ef3bc83e8351d59ea9fa986
Author: Shenglei-Mao <mao59@purdue.edu>
Date:   Tue Apr 28 14:32:08 2020 -0400

    2 sites working very stable now

commit def196a72ef62a8e6d4e4d05f340b56cfc76b653
Author: Shenglei-Mao <mao59@purdue.edu>
Date:   Tue Apr 28 12:58:27 2020 -0400

    serialized
```
Commit "ac3d" is a good demo for 4 sites but with less proper log system
<br />Commit "d932" is a good demo for 2 sites with a better logging system

###Deployment Instruction
Run Central, Site0, Site1, Site3 script

###Grpc python-gen
```shell script
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. global_validation.proto
```

###Log Location
```shell script
under ./log/
```
Highly suggest first clean the log and then start a new run

###More Info
check out the report