use tokio::sync::mpsc;

use crate::error::Result;

/// An event with associated id and client id.
#[derive(Debug, Clone)]
pub struct Event {
    pub id: u64,
    pub client_id: u64,
    pub event: OperationEvent,
}

/// An operation event.
#[derive(Debug, Clone)]
pub enum OperationEvent {
    PushStart { num_rows: usize, last_value: u64 },
    PushEnd { end_offset: u64 },
    FetchStart { offset: u64 },
    FetchEnd { offset: u64, value: u64 },
}

pub async fn run_log_loop(mut rx: mpsc::Receiver<Event>) -> Result<()> {
    while let Some(event) = rx.recv().await {
        println!("{}", event);
    }

    Ok(())
}

impl std::fmt::Display for Event {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:<4}|{:<4}|{}", self.client_id, self.id, self.event)
    }
}

impl std::fmt::Display for OperationEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OperationEvent::PushStart {
                num_rows,
                last_value,
            } => {
                write!(
                    f,
                    "START|PUSH |{:<8}|{:<24}|{:<4}",
                    "", last_value, num_rows
                )
            }
            OperationEvent::PushEnd { end_offset } => {
                write!(f, "END  |PUSH |{:<8}|{:<24}|", end_offset, "")
            }
            OperationEvent::FetchStart { offset } => {
                write!(f, "START|FETCH|{:<8}|{:<24}|", offset, "")
            }
            OperationEvent::FetchEnd { offset, value } => {
                write!(f, "END  |FETCH|{:<8}|{:<24}|", offset, value)
            }
        }
    }
}
