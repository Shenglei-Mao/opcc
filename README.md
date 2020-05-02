Centralized Optimistic Concurrency Control

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

### Things to notice
