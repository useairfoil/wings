use std::{error::Error, sync::Arc, time::Duration};

use clap::Args;
use prost::Message;
use rand::RngExt;
use throttled_tracing::info_every;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::info;
use wings::object_store::ObjectStoreArgs;
use wings_common::{clock::SystemClock, id::IdGenerator};
use wings_queue::{QueueClient, pb};

type StressResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

const PRODUCER_PAYLOAD_TYPE_URL: &str = "type.googleapis.com/wings.stress.ProducerTask";
const PROGRESS_LOG_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Debug, Args)]
pub struct QueueArgs {
    #[command(flatten)]
    pub object_store: ObjectStoreArgs,
    #[command(flatten)]
    pub producer: ProducerArgs,
    #[command(flatten)]
    pub worker: WorkerArgs,
}

#[derive(Debug, Clone, Args)]
pub struct ProducerArgs {
    /// Enable the producer task.
    #[arg(long = "producer.enable", default_value_t = false)]
    pub producer_enable: bool,
    /// The number of tasks (per second) pushed to the queue.
    #[arg(long = "producer.rate", default_value_t = 100)]
    pub producer_rate: u64,
    /// The number of tasks to push to the queue.
    #[arg(long = "producer.count", default_value_t = u64::MAX)]
    pub producer_count: u64,
    /// The size (in bytes) of each task to push to the queue.
    #[arg(long = "producer.task-size", default_value_t = 1024)]
    pub producer_task_size: usize,
    /// The number of unique tasks to push to the queue.
    #[arg(long = "producer.num-unique", default_value_t = 100)]
    pub producer_num_unique: u64,
}

#[derive(Debug, Clone, Args)]
pub struct WorkerArgs {
    /// Enable the worker task.
    #[arg(long = "worker.enable", default_value_t = false)]
    pub worker_enable: bool,
    /// The number of tasks (per second) handled by the worker.
    #[arg(long = "worker.rate", default_value_t = 100)]
    pub worker_rate: u64,
    /// The number of tasks to pull from the queue at a time.
    #[arg(long = "worker.num-tasks", default_value_t = 10)]
    pub worker_num_tasks: u32,
    /// The percentage of tasks completed as failed, between 0.0 and 1.0.
    #[arg(long = "worker.error-rate", default_value_t = 0.0)]
    pub worker_error_rate: f64,
}

#[derive(Clone, PartialEq, Message)]
struct ProducerTask {
    #[prost(bytes = "vec", tag = "1")]
    data: Vec<u8>,
}

impl QueueArgs {
    pub async fn run(
        self,
        clock: Arc<dyn SystemClock>,
        ct: CancellationToken,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let store = self.object_store.create_object_store()?;

        let client = QueueClient::init(clock, store, Default::default()).await?;

        let mut tasks = JoinSet::new();

        if self.producer.producer_enable {
            tasks.spawn(run_producer(client.clone(), self.producer, ct.clone()));
        }

        if self.worker.worker_enable {
            tasks.spawn(run_worker(client.clone(), self.worker, ct.clone()));
        }

        if tasks.is_empty() {
            info!("no queue stress tasks enabled");
            return Ok(());
        }

        while let Some(result) = tasks.join_next().await {
            result??;
        }

        Ok(())
    }
}

async fn run_worker(
    client: QueueClient,
    args: WorkerArgs,
    ct: CancellationToken,
) -> StressResult<()> {
    let worker_rate = args.worker_rate.max(1);
    let worker_num_tasks = args.worker_num_tasks.max(1);

    if !(0.0..=1.0).contains(&args.worker_error_rate) {
        return Err("worker.error-rate must be between 0.0 and 1.0".into());
    }

    let interval = Duration::from_nanos((1_000_000_000 / worker_rate).max(1));
    let max_in_flight = usize::try_from(worker_rate).unwrap_or(usize::MAX).max(1);
    let worker_id = rand::rng().gen_uuid().to_string();
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    let mut tasks = Vec::new();
    let mut in_flight = JoinSet::new();
    let mut handled = 0_u64;

    info!(
        worker_id,
        rate = worker_rate,
        num_tasks = worker_num_tasks,
        error_rate = args.worker_error_rate,
        max_in_flight,
        "queue worker started"
    );

    loop {
        info_every!(
            PROGRESS_LOG_INTERVAL,
            worker_id,
            handled,
            being_handled = in_flight.len(),
            buffered = tasks.len(),
            "queue worker progress"
        );

        tokio::select! {
            _ = ct.cancelled() => {
                info!(worker_id, handled, "queue worker cancelled");
                drain_in_flight(&mut in_flight).await?;
                return Ok(());
            }
            _ = ticker.tick() => {}
        }

        while in_flight.len() >= max_in_flight {
            tokio::select! {
                _ = ct.cancelled() => {
                    info!(worker_id, handled, "queue worker cancelled");
                    drain_in_flight(&mut in_flight).await?;
                    return Ok(());
                }
                result = in_flight.join_next() => join_task(result)?,
            }
        }

        if tasks.is_empty() {
            let response = client
                .request_tasks(pb::RequestTasksRequest {
                    worker_id: worker_id.clone(),
                    max_tasks: worker_num_tasks,
                    lease_duration: Some(prost_types::Duration {
                        seconds: 30,
                        nanos: 0,
                    }),
                })
                .await?;

            tasks = response.tasks;
            tasks.reverse();

            if tasks.is_empty() {
                continue;
            }
        }

        let task = tasks.pop().expect("tasks is not empty");
        let failed = rand::random::<f64>() < args.worker_error_rate;
        let request = new_acknowledge_task_request(worker_id.clone(), task.task_id, failed);
        let client = client.clone();

        in_flight.spawn(async move {
            client.acknowledge_task(request).await?;
            Ok::<_, Box<dyn Error + Send + Sync>>(())
        });

        handled += 1;
    }
}

async fn run_producer(
    client: QueueClient,
    args: ProducerArgs,
    ct: CancellationToken,
) -> StressResult<()> {
    let producer_rate = args.producer_rate.max(1);
    let producer_num_unique = args.producer_num_unique.max(1);

    let interval = Duration::from_nanos((1_000_000_000 / producer_rate).max(1));
    let max_in_flight = usize::try_from(producer_rate).unwrap_or(usize::MAX).max(1);
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    let mut in_flight = JoinSet::new();
    let mut produced = 0;

    info!(
        rate = producer_rate,
        count = args.producer_count,
        task_size = args.producer_task_size,
        num_unique = producer_num_unique,
        max_in_flight,
        "queue producer started"
    );

    while produced < args.producer_count {
        info_every!(
            PROGRESS_LOG_INTERVAL,
            produced,
            in_flight = in_flight.len(),
            "queue producer progress"
        );

        tokio::select! {
            _ = ct.cancelled() => {
                info!(produced, "queue producer cancelled");
                break;
            }
            _ = ticker.tick() => {}
        }

        while in_flight.len() >= max_in_flight {
            tokio::select! {
                _ = ct.cancelled() => {
                    info!(produced, "queue producer cancelled");
                    drain_in_flight(&mut in_flight).await?;
                    return Ok(());
                }
                result = in_flight.join_next() => join_task(result)?,
            }
        }

        let request = new_schedule_task_request(args.producer_task_size, args.producer_num_unique)?;
        let client = client.clone();

        in_flight.spawn(async move {
            client.schedule_task(request).await?;
            Ok::<_, Box<dyn Error + Send + Sync>>(())
        });

        produced += 1;
    }

    drain_in_flight(&mut in_flight).await?;

    info!(produced, "queue producer finished");

    Ok(())
}

fn new_acknowledge_task_request(
    worker_id: String,
    task_id: String,
    failed: bool,
) -> pb::AcknowledgeTaskRequest {
    let result = if failed {
        pb::acknowledge_task_request::Result::Failure(pb::TaskFailure {
            error_message: "stress worker failure".to_string(),
            details: None,
            retry_at: None,
        })
    } else {
        pb::acknowledge_task_request::Result::Success(pb::TaskSuccess {})
    };

    pb::AcknowledgeTaskRequest {
        worker_id,
        task_id,
        result: Some(result),
    }
}

fn new_schedule_task_request(
    task_size: usize,
    num_unique: u64,
) -> StressResult<pb::ScheduleTaskRequest> {
    let mut rng = rand::rng();
    let mut data = vec![0; task_size];
    rng.fill(&mut data);

    let payload = ProducerTask { data };
    let mut value = Vec::with_capacity(payload.encoded_len());
    payload.encode(&mut value)?;

    Ok(pb::ScheduleTaskRequest {
        unique_id: rng.random_range(0..num_unique).to_string(),
        payload: Some(prost_types::Any {
            type_url: PRODUCER_PAYLOAD_TYPE_URL.to_string(),
            value,
        }),
        run_at: None,
    })
}

async fn drain_in_flight(in_flight: &mut JoinSet<StressResult<()>>) -> StressResult<()> {
    while let Some(result) = in_flight.join_next().await {
        join_task(Some(result))?;
    }

    Ok(())
}

fn join_task(result: Option<Result<StressResult<()>, tokio::task::JoinError>>) -> StressResult<()> {
    if let Some(result) = result {
        result??;
    }

    Ok(())
}
