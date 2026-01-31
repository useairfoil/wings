use wings_resources::ResourceError;

pub fn resource_error_to_status(resource: &str, error: ResourceError) -> tonic::Status {
    use tonic::Status;

    match error {
        ResourceError::InvalidFormat { expected, actual } => Status::invalid_argument(format!(
            "invalid {resource} name format: expected '{expected}' but got '{actual}'",
        )),
        ResourceError::InvalidName { name } => {
            Status::invalid_argument(format!("invalid {resource} name: {name}"))
        }
        ResourceError::MissingParent { name } => {
            Status::invalid_argument(format!("missing parent {resource} in name: {name}"))
        }
        ResourceError::InvalidResourceId { id } => {
            Status::invalid_argument(format!("invalid {resource} id: {id}"))
        }
    }
}
