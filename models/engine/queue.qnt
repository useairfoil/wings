// Model of "queue.json with an HA brokered group commit" from:
// https://turbopuffer.com/blog/object-storage-queue#step-4-queuejson-with-an-ha-brokered-group-commit
//
// The article's clients send push, claim, heartbeat, and completion requests to
// a stateless broker. The broker batches those requests and atomically replaces
// queue.json with an ETag-checked object-storage write. queue.json is the durable
// source of truth; broker caches and buffers are volatile.
module queue {
  import object_storage as s3 from "./object_storage"

  type BrokerId = str
  type WorkerId = str
  type JobId = str
  type Timestamp = int

  type ClaimInfo = { worker: WorkerId, heartbeat: Timestamp }
  type JobStatus = Pending | Claimed(ClaimInfo)
  type Job = { id: JobId, status: JobStatus }

  // This is the modeled content of the article's single queue.json object.
  // `completed` makes finished jobs observable for the durability property;
  // pending and claimed jobs remain in FIFO order in `jobs`.
  type QueueJson = { broker: BrokerId, jobs: List[Job], completed: Set[JobId] }

  // Requests correspond to the article's client operations. They first enter a
  // broker's in-memory group-commit buffer and are durable only after commit.
  type ClaimRequest = { worker: WorkerId, now: Timestamp }
  type HeartbeatRequest = { jobId: JobId, worker: WorkerId, now: Timestamp }
  type CompleteRequest = { jobId: JobId, worker: WorkerId }
  type Request =
    | Push(JobId)
    | Claim(ClaimRequest)
    | Heartbeat(HeartbeatRequest)
    | Complete(CompleteRequest)

  pure val QUEUE_KEY: str = "queue.json"
  pure val HEARTBEAT_TIMEOUT = 3

  // A broker is stateless in the article: these caches and buffers model only
  // volatile process memory and may be discarded when the broker stops.
  var brokerCaches: BrokerId -> s3::StoredObject[QueueJson]
  var brokerBuffers: BrokerId -> List[Request]
  var aliveBrokers: Set[BrokerId]
  var acknowledgedJobs: Set[JobId]
  var clock: Timestamp

  pure def emptyQueue(broker: BrokerId): QueueJson =
    { broker: broker, jobs: [], completed: Set() }

  // The article allows another worker to take over a claimed job after its
  // heartbeat becomes older than the configured timeout.
  pure def isAvailable(status: JobStatus, now: Timestamp): bool =
    match status {
      | Pending => true
      | Claimed(info) => now - info.heartbeat > HEARTBEAT_TIMEOUT
    }

  // Claim the first available job, preserving the article's FIFO execution.
  pure def claimJob(queue: QueueJson, worker: WorkerId, now: Timestamp): QueueJson = {
    val available = queue.jobs.select(job => isAvailable(job.status, now))
    if (available.length() == 0) queue
    else {
      val claimedId = available.head().id
      {
        broker: queue.broker,
        jobs: queue.jobs.foldl([], (jobs, job) =>
          jobs.append(
            if (job.id == claimedId)
              { id: job.id, status: Claimed({ worker: worker, heartbeat: now }) }
            else job
          )
        ),
        completed: queue.completed,
      }
    }
  }

  // A heartbeat renews a claim only when sent by the worker that owns it.
  pure def heartbeatJob(
    queue: QueueJson,
    jobId: JobId,
    worker: WorkerId,
    now: Timestamp,
  ): QueueJson = {
    val jobs = queue.jobs.foldl([], (jobs, job) =>
      jobs.append(
        if (job.id != jobId) job
        else match job.status {
          | Pending => job
          | Claimed(info) =>
              if (info.worker == worker)
                { id: job.id, status: Claimed({ worker: worker, heartbeat: now }) }
              else job
        }
      )
    )
    { broker: queue.broker, jobs: jobs, completed: queue.completed }
  }

  pure def isOwned(job: Job, jobId: JobId, worker: WorkerId): bool =
    job.id == jobId and match job.status {
      | Pending => false
      | Claimed(info) => info.worker == worker
    }

  // Completion removes the claimed job from the queue. Recording its ID lets
  // the model retain evidence that an acknowledged job was durably processed.
  pure def completeJob(queue: QueueJson, jobId: JobId, worker: WorkerId): QueueJson = {
    val owned = queue.jobs.foldl(
      false,
      (found, job) => found or isOwned(job, jobId, worker)
    )
    {
      broker: queue.broker,
      jobs: queue.jobs.select(job => not(isOwned(job, jobId, worker))),
      completed: if (owned) queue.completed.union(Set(jobId)) else queue.completed,
    }
  }

  pure def applyRequest(queue: QueueJson, request: Request): QueueJson =
    match request {
      | Push(jobId) => {
          broker: queue.broker,
          jobs: queue.jobs.append({ id: jobId, status: Pending }),
          completed: queue.completed,
        }
      | Claim(request) => claimJob(queue, request.worker, request.now)
      | Heartbeat(request) => heartbeatJob(queue, request.jobId, request.worker, request.now)
      | Complete(request) => completeJob(queue, request.jobId, request.worker)
    }

  // Fold the broker buffer in arrival order to form one replacement queue.json.
  pure def applyRequests(queue: QueueJson, requests: List[Request]): QueueJson =
    requests.foldl(queue, (nextQueue, request) => applyRequest(nextQueue, request))

  pure def pushedJobs(requests: List[Request]): Set[JobId] =
    requests.foldl(Set(), (jobs, request) => match request {
      | Push(jobId) => jobs.union(Set(jobId))
      | Claim(_) => jobs
      | Heartbeat(_) => jobs
      | Complete(_) => jobs
    })

  val queueObject: s3::StoredObject[QueueJson] = s3::getObject(QUEUE_KEY)

  // Bootstrap queue.json and the first broker. Later broker processes discover
  // the current address by reading this object, as described in Step 4.
  action init(initialBroker: BrokerId): bool = {
    val queue = emptyQueue(initialBroker)
    val stored = { data: queue, etag: 0 }
    all {
      s3::objects' = Map(QUEUE_KEY -> stored),
      s3::nextETag' = 1,
      brokerCaches' = Map(initialBroker -> stored),
      brokerBuffers' = Map(initialBroker -> []),
      aliveBrokers' = Set(initialBroker),
      acknowledgedJobs' = Set(),
      clock' = 0,
    }
  }

  // Accept a client request into volatile memory only at the broker whose
  // address is currently published in queue.json. This does not acknowledge it.
  action enqueue(broker: BrokerId, request: Request): bool = all {
    aliveBrokers.contains(broker),
    queueObject.data.broker == broker,
    brokerBuffers.keys().contains(broker),
    s3::objects' = s3::objects,
    s3::nextETag' = s3::nextETag,
    brokerCaches' = brokerCaches,
    brokerBuffers' = brokerBuffers.put(broker, brokerBuffers.get(broker).append(request)),
    aliveBrokers' = aliveBrokers,
    acknowledgedJobs' = acknowledgedJobs,
    clock' = clock,
  }

  action enqueuePush(broker: BrokerId, jobId: JobId): bool = enqueue(broker, Push(jobId))
  action enqueueClaim(broker: BrokerId, worker: WorkerId): bool =
    enqueue(broker, Claim({ worker: worker, now: clock }))
  action enqueueHeartbeat(broker: BrokerId, jobId: JobId, worker: WorkerId): bool =
    enqueue(broker, Heartbeat({ jobId: jobId, worker: worker, now: clock }))
  action enqueueComplete(broker: BrokerId, jobId: JobId, worker: WorkerId): bool =
    enqueue(broker, Complete({ jobId: jobId, worker: worker }))

  // The article's brokered group commit: apply the complete buffer and perform
  // one CAS replacement using the ETag read into the broker cache. Pushes are
  // acknowledged only after that write succeeds and therefore become durable.
  action groupCommit(broker: BrokerId): bool = {
    val cached = brokerCaches.get(broker)
    val requests = brokerBuffers.get(broker)
    val nextQueue = applyRequests(cached.data, requests)
    all {
      aliveBrokers.contains(broker),
      requests.length() > 0,
      cached.data.broker == broker,
      s3::putObject(QUEUE_KEY, nextQueue, s3::Update(cached.etag)),
      brokerCaches' = brokerCaches.put(
        broker,
        { data: nextQueue, etag: s3::nextETag }
      ),
      brokerBuffers' = brokerBuffers.put(broker, []),
      aliveBrokers' = aliveBrokers,
      acknowledgedJobs' = acknowledgedJobs.union(pushedJobs(requests)),
      clock' = clock,
    }
  }

  // A prospective replacement broker starts by reading queue.json. Multiple
  // brokers may overlap temporarily; CAS, rather than process exclusivity,
  // preserves correctness during failover.
  action startBroker(broker: BrokerId): bool = all {
    not(aliveBrokers.contains(broker)),
    s3::objects' = s3::objects,
    s3::nextETag' = s3::nextETag,
    brokerCaches' = brokerCaches.put(broker, queueObject),
    brokerBuffers' = brokerBuffers.put(broker, []),
    aliveBrokers' = aliveBrokers.union(Set(broker)),
    acknowledgedJobs' = acknowledgedJobs,
    clock' = clock,
  }

  // After losing a CAS race, a broker rereads queue.json before retrying.
  action refreshBroker(broker: BrokerId): bool = all {
    aliveBrokers.contains(broker),
    s3::objects' = s3::objects,
    s3::nextETag' = s3::nextETag,
    brokerCaches' = brokerCaches.put(broker, queueObject),
    brokerBuffers' = brokerBuffers,
    aliveBrokers' = aliveBrokers,
    acknowledgedJobs' = acknowledgedJobs,
    clock' = clock,
  }

  // Publish the replacement broker's address with CAS. A stale candidate fails
  // instead of overwriting changes committed since its read.
  action electBroker(broker: BrokerId): bool = {
    val cached = brokerCaches.get(broker)
    val nextQueue = {
      broker: broker,
      jobs: cached.data.jobs,
      completed: cached.data.completed,
    }
    all {
      aliveBrokers.contains(broker),
      s3::putObject(QUEUE_KEY, nextQueue, s3::Update(cached.etag)),
      brokerCaches' = brokerCaches.put(
        broker,
        { data: nextQueue, etag: s3::nextETag }
      ),
      brokerBuffers' = brokerBuffers,
      aliveBrokers' = aliveBrokers,
      acknowledgedJobs' = acknowledgedJobs,
      clock' = clock,
    }
  }

  // Broker failure discards its volatile buffer. This is safe because the
  // article requires responses only after requests have landed in storage.
  action stopBroker(broker: BrokerId): bool = all {
    aliveBrokers.contains(broker),
    s3::objects' = s3::objects,
    s3::nextETag' = s3::nextETag,
    brokerCaches' = brokerCaches,
    brokerBuffers' = brokerBuffers.put(broker, []),
    aliveBrokers' = aliveBrokers.exclude(Set(broker)),
    acknowledgedJobs' = acknowledgedJobs,
    clock' = clock,
  }

  // Logical time is used only to model heartbeat expiry and job takeover.
  action advanceTime(newTime: Timestamp): bool = all {
    newTime > clock,
    s3::objects' = s3::objects,
    s3::nextETag' = s3::nextETag,
    brokerCaches' = brokerCaches,
    brokerBuffers' = brokerBuffers,
    aliveBrokers' = aliveBrokers,
    acknowledgedJobs' = acknowledgedJobs,
    clock' = newTime,
  }

  val durableJobIds: Set[JobId] =
    queueObject.data.jobs.foldl(
      queueObject.data.completed,
      (jobIds, job) => jobIds.union(Set(job.id))
    )

  // Central safety property behind the article's acknowledgement rule: every
  // acknowledged push is still queued or has been durably completed.
  val acknowledgedJobsAreDurable: bool = acknowledgedJobs.subseteq(durableJobIds)
}

module queue_tests {
  import queue.*

  // Step 3/4 group commit: two buffered pushes land in one queue.json CAS write
  // and are acknowledged only after the durable object contains both jobs.
  run groupCommitTest =
    init("broker-1")
      .then(enqueuePush("broker-1", "job-1"))
      .then(enqueuePush("broker-1", "job-2"))
      .then(groupCommit("broker-1"))
      .expect(
        queueObject.data.jobs.foldl([], (jobIds, job) => jobIds.append(job.id)) ==
          ["job-1", "job-2"] and
        queueObject.etag == 1 and
        brokerBuffers.get("broker-1") == [] and
        acknowledgedJobs == Set("job-1", "job-2") and
        acknowledgedJobsAreDurable
      )

  // Step 4 broker failure: a buffered request may disappear, but because no
  // group commit landed, the client has not received a durable acknowledgement.
  run brokerCrashLosesOnlyUnacknowledgedRequestsTest =
    init("broker-1")
      .then(enqueuePush("broker-1", "job-1"))
      .then(stopBroker("broker-1"))
      .expect(
        queueObject.data.jobs == [] and
        acknowledgedJobs == Set() and
        brokerBuffers.get("broker-1") == []
      )

  // Step 4 overlapping brokers: after broker-2 publishes itself, broker-1's
  // stale ETag prevents it from committing its old in-memory buffer.
  run brokerFailoverRejectsStaleCommitTest =
    init("broker-1")
      .then(enqueuePush("broker-1", "job-1"))
      .then(startBroker("broker-2"))
      .then(electBroker("broker-2"))
      .expect(queueObject.data.broker == "broker-2" and queueObject.data.jobs == [])
      .then(groupCommit("broker-1").fail())

  // A candidate that loses a broker-election CAS race rereads queue.json and
  // can then retry with the latest ETag, matching the article's CAS retry loop.
  run brokerRefreshesBeforeRetryingElectionTest =
    init("broker-1")
      .then(startBroker("broker-2"))
      .then(startBroker("broker-3"))
      .then(electBroker("broker-2"))
      .then(refreshBroker("broker-3"))
      .then(electBroker("broker-3"))
      .expect(queueObject.data.broker == "broker-3")

  // Workers claim the first available jobs in queue order, providing the FIFO
  // execution promised by the article.
  run claimsAreFifoTest =
    init("broker-1")
      .then(enqueuePush("broker-1", "job-1"))
      .then(enqueuePush("broker-1", "job-2"))
      .then(groupCommit("broker-1"))
      .then(enqueueClaim("broker-1", "worker-1"))
      .then(enqueueClaim("broker-1", "worker-2"))
      .then(groupCommit("broker-1"))
      .expect(
        queueObject.data.jobs == [
          {
            id: "job-1",
            status: Claimed({ worker: "worker-1", heartbeat: 0 }),
          },
          {
            id: "job-2",
            status: Claimed({ worker: "worker-2", heartbeat: 0 }),
          },
        ]
      )

  // A claimed job's periodic heartbeat is group-committed into queue.json so
  // other workers know that its current owner is still making progress.
  run claimAndHeartbeatTest =
    init("broker-1")
      .then(enqueuePush("broker-1", "job-1"))
      .then(groupCommit("broker-1"))
      .then(enqueueClaim("broker-1", "worker-1"))
      .then(groupCommit("broker-1"))
      .then(advanceTime(2))
      .then(enqueueHeartbeat("broker-1", "job-1", "worker-1"))
      .then(groupCommit("broker-1"))
      .expect(
        queueObject.data.jobs.head().status ==
          Claimed({ worker: "worker-1", heartbeat: 2 })
      )

  // Step 4 worker failure: once the last heartbeat exceeds the timeout, the
  // next worker takes over the job, giving at-least-once execution.
  run staleClaimIsReassignedTest =
    init("broker-1")
      .then(enqueuePush("broker-1", "job-1"))
      .then(groupCommit("broker-1"))
      .then(enqueueClaim("broker-1", "worker-1"))
      .then(groupCommit("broker-1"))
      .then(advanceTime(4))
      .then(enqueueClaim("broker-1", "worker-2"))
      .then(groupCommit("broker-1"))
      .expect(
        queueObject.data.jobs.head().status ==
          Claimed({ worker: "worker-2", heartbeat: 4 })
      )

  // A stale or unrelated worker cannot complete another worker's live claim;
  // the owning worker can remove it while preserving acknowledged durability.
  run onlyCurrentWorkerCanCompleteTest =
    init("broker-1")
      .then(enqueuePush("broker-1", "job-1"))
      .then(groupCommit("broker-1"))
      .then(enqueueClaim("broker-1", "worker-1"))
      .then(groupCommit("broker-1"))
      .then(enqueueComplete("broker-1", "job-1", "worker-2"))
      .then(groupCommit("broker-1"))
      .expect(queueObject.data.jobs.length() == 1)
      .then(enqueueComplete("broker-1", "job-1", "worker-1"))
      .then(groupCommit("broker-1"))
      .expect(
        queueObject.data.jobs == [] and
        queueObject.data.completed == Set("job-1") and
        acknowledgedJobsAreDurable
      )
}

// Bounded harness used by simulation and model checking. The queue module's
// parameterized actions remain the source of truth; this module only supplies
// finite broker, worker, job, and time domains plus init/step entry points.
module queue_verification {
  import queue.*

  pure val BROKERS: Set[BrokerId] = Set("broker-1", "broker-2")
  pure val WORKERS: Set[WorkerId] = Set("worker-1", "worker-2")
  pure val JOBS: Set[JobId] = Set("job-1", "job-2")
  pure val TIMES: Set[Timestamp] = Set(1, 2, 3, 4)

  action verificationInit = init("broker-1")

  action verificationStep = {
    // brokerBuffers always contains the initial broker, including after it is
    // stopped, so selecting a known broker never evaluates an undefined map
    // lookup in the underlying actions.
    nondet knownBroker = brokerBuffers.keys().oneOf()
    nondet candidateBroker = BROKERS.oneOf()
    nondet worker = WORKERS.oneOf()
    nondet job = JOBS.oneOf()
    nondet time = TIMES.oneOf()
    any {
      enqueuePush(knownBroker, job),
      enqueueClaim(knownBroker, worker),
      enqueueHeartbeat(knownBroker, job, worker),
      enqueueComplete(knownBroker, job, worker),
      groupCommit(knownBroker),
      startBroker(candidateBroker),
      refreshBroker(knownBroker),
      electBroker(knownBroker),
      stopBroker(knownBroker),
      advanceTime(time),
    }
  }

  val verificationInvariants: bool = acknowledgedJobsAreDurable
}
