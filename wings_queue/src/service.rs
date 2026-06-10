use tonic::{Request, Response, Status};

use crate::pb;

pub struct QueueBrokerService {}

impl QueueBrokerService {
    pub fn new() -> Self {
        Self {}
    }

    pub fn into_service(self) -> pb::broker_server::BrokerServer<Self> {
        pb::broker_server::BrokerServer::new(self)
    }
}

#[tonic::async_trait]
impl pb::broker_server::Broker for QueueBrokerService {
    async fn schedule_task(
        &self,
        request: Request<pb::ScheduleTaskRequest>,
    ) -> Result<Response<pb::ScheduleTaskResponse>, Status> {
        todo!()
    }

    async fn request_tasks(
        &self,
        request: Request<pb::RequestTasksRequest>,
    ) -> Result<Response<pb::RequestTasksResponse>, Status> {
        todo!()
    }

    async fn acknowledge_task(
        &self,
        request: Request<pb::AcknowledgeTaskRequest>,
    ) -> Result<Response<pb::AcknowledgeTaskResponse>, Status> {
        todo!()
    }

    async fn heartbeat(
        &self,
        request: Request<pb::HeartbeatRequest>,
    ) -> Result<Response<pb::HeartbeatResponse>, Status> {
        todo!()
    }
}
