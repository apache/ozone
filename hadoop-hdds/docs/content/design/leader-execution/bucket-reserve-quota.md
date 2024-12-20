# Bucket quota

Earlier, bucket level lock is taken, quota validation is performed and updated with-in lock in cache in all nodes.
During startup before persistence to db, request is re-executed from ratis log and bucket quota cache is prepared again.
So once bucket quota is updated in cache, it will remain same (As recovered during startup with same re-execution).

Now request is getting executed at leader node, so bucket case will not be able to recover if crash happens. So it can be updated in BucketTable cache only after its persisted.

![quota_reserve_flow.png](quota_reserve_flow.png)

For bucket quota in new flow,
- When processing key commit, the quota will be `reserved` at leader.
- Bucket quota changes will be distributed to all other nodes via ratis
- At all nodes, key changes is flushed to DB, during that time, quota change will be updated to BucketTable, and quota reserve will be reset.
- On failure, reserve quota for the request will be reset.

`Bucket Resource Quota` will store quota information with respect to `index` also and same will be used to reset on request handling,
- At leader node after request is send to ratis in success and failure path (as default always) with `request index`
- At all nodes on apply transaction, quota is reset with request index.
  So in all cases, reserved quota can be removed in processing of request.

Cases:
1. Quota is reserved at leader node but sync to other nodes fail, quota will be reset always
2. Quota is updated at leader node in apply transaction, it will reset quota to avoid double quota increase
3. Quota is updated at follower node in apply transaction, reset as no impact as `Bucket Quota resource` will not have any entry for the the request
