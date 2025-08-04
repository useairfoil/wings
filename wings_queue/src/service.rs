use std::sync::Arc;

use object_store::ObjectStore;
use thiserror::Error;
use tonic::{Request, Response, Status};
use uuid::Uuid;
use wings_common::clock::SystemClock;

use crate::{Header, ManifestStore, pb};

#[derive(Debug)]
pub struct QueueBrokerService {
    clock: Arc<dyn SystemClock>,
}

#[derive(Debug, Error)]
pub enum Error {}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl QueueBrokerService {
    pub async fn init(
        clock: Arc<dyn SystemClock>,
        object_store: Arc<dyn ObjectStore>,
        header: Header,
    ) -> Result<Self> {
        let store = ManifestStore::new(object_store)
            .load_or_init(header)
            .await
            .unwrap();

        Ok(Self { clock })
    }

    pub fn into_service(self) -> pb::broker_server::BrokerServer<Self> {
        pb::broker_server::BrokerServer::new(self)
    }
}

#[tonic::async_trait]
impl pb::broker_server::Broker for QueueBrokerService {
    async fn schedule_task(
        &self,
        _request: Request<pb::ScheduleTaskRequest>,
    ) -> Result<Response<pb::ScheduleTaskResponse>, Status> {
        let task_id = Uuid::now_v7(); // TODO: based on clock
        let response = pb::ScheduleTaskResponse {
            task_id: task_id.to_string(),
        };

        Ok(Response::new(response))
    }

    async fn request_tasks(
        &self,
        request: Request<pb::RequestTasksRequest>,
    ) -> Result<Response<pb::RequestTasksResponse>, Status> {
        let response = pb::RequestTasksResponse { tasks: vec![] };

        Ok(Response::new(response))
    }

    async fn acknowledge_task(
        &self,
        request: Request<pb::AcknowledgeTaskRequest>,
    ) -> Result<Response<pb::AcknowledgeTaskResponse>, Status> {
        let response = pb::AcknowledgeTaskResponse {};

        Ok(Response::new(response))
    }

    async fn heartbeat(
        &self,
        request: Request<pb::HeartbeatRequest>,
    ) -> Result<Response<pb::HeartbeatResponse>, Status> {
        Ok(pb::HeartbeatResponse {}.into())
    }
}
