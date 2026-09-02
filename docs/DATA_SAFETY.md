Introduction:

- the cluster uses quorum replication, `fsync=on`, and `synchronous_commit=on/remote_apply`;
- we store the quorum in ZK and in the primary's SSN;
- the quorum changes only through the quorum change algorithm;
- manually changing SSN, recovery sources, timeline, and the corresponding ZK records is prohibited;
- `application_name` uniquely identifies trusted replicas;
- the guarantees do not apply to `--with-data-loss`;
- the ordinary quorum part of the primary's SSN must correspond to at least one of the two quorums recorded in ZK; maintenance is currently an exception;
- in patched switchover, SSN is strengthened to `EVERY(C), ANY W(D0)(R(D0,P))`, but the `D0` host set does not change, so failover can rely on the data in ZK.

Quorum change:

- happens one host at a time;
- write the desired SSN to ZK;
- set the SSN;
- commit a transaction to the quorum with the new SSN to make sure that it has been applied;
- mark the desired SSN as applied;
- thus, failover knows all SSNs that could actually have been in effect at the time of failure.

Failover:

- replicas vote only after they stop receiving WAL from all sources: archive and replication;
- using `lwaldump`, read the replica's current position from the WAL on the filesystem. Applied WAL may lag, and after a restart its counters may move backward, although the WAL on the filesystem remains;
- vote with the resulting LSN and timeline, as well as the unique voting identifier;
- check that there are enough votes for all possible quorum variants recorded in ZK;
- if there are not enough votes, wait;
- the host that is guaranteed to contain all transactions whose commit could have been confirmed to the client under any quorum that could have been applied wins. First, the unique voting IDs are compared, the timeline selects the branch, and the LSN selects the winner within it;
- set SSN on the selected primary, promote it, and point the replicas to it;
- in a failover started on top of a running switchover, check whether a commit could have existed on the candidate's timeline. If it could, candidates and LSNs are compared only on the new timeline. Votes from the old timeline may be counted toward the read quorum because they prove that the corresponding hosts are fenced and do not contain the new commit. If it is proven that a commit on the new timeline was impossible, the old branch remains safe. Returning specifically to `P` is an optimization: `P` is guaranteed to contain the data from the old branch, so a separate winner election on that branch is unnecessary.

Switchover:

- the new primary is made a synchronous replica, while the quorum host set does not change: `SSN = EVERY(new_master), ANY W(quorum)`;
- commit a transaction on the old primary to make sure that the configuration has been applied;
- set SSN on the new primary;
- after preparation, perform the following actions in an order that is irrelevant to data safety: promote the new primary, shut down the old one, and point the remaining hosts to the new primary.
