# Centralized Optimistic Concurrency Control
### Key algorithm
##### local-validation phase:<br />
transaction local validate against 1. all the committed transaction that have end time later(strictly larger) than
the start time of current transaction; 2. all the semi_committed_transactions
For the above transactions x and this transaction y
1. if the write set of x overlap with the read set y, y needs to redo it
2. if x is a semi_committed_transaction, if x's write set overlap with the write set of y (not necessary)

##### global-validation phase:
assume clock is well synced, called central site method for validate using grpc
validate against all the global committed transaction & central site's semi-committed transaction

##### Redo Algorithm: 
A separate thread running for collecting all the transactions and retry the most weighted transaction first. 
(Actually, not a very good idea since those transaction failed more times would probably fail again and again so 
that they may never get committed) A better way to fix that is to use "ghost method"- if a long transaction T fail 
too many times, then abort those who conflict with T instead of playing a fair game and commit short transaction first

##### Fully Replicated Database
Each transaction get a unique id when going through the global validation process, since only the central site were 
given out this id, the id is unique actually at a global scope, so even when message arrive to the remote sites out 
of order, we could still use this id to make it ordered and keep the database consistency across sites, and this is 
efficiently implanted by the  wait and notifyall mechanism.

### Log Location
```shell script
under ./log/
```

### More Info
check out the report
