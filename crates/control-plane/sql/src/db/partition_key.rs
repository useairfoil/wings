use wings_resources::{PartitionValue, TableName};

/// Represents a partition's key in the database.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PartitionKey {
    pub tenant_id: String,
    pub namespace_id: String,
    pub table_id: String,
    pub partition_value: Vec<u8>,
}

impl PartitionKey {
    pub fn new(name: &TableName, partition_value: Option<PartitionValue>) -> Self {
        use prost::Message;
        use wings_control_plane_core::pb::PartitionValue as Proto;

        let namespace = name.parent();
        let tenant = namespace.parent();

        let pv = if let Some(ref pv) = partition_value {
            let pv: Proto = pv.into();
            pv.encode_to_vec()
        } else {
            Vec::default()
        };

        Self {
            tenant_id: tenant.id().to_owned(),
            namespace_id: namespace.id().to_owned(),
            table_id: name.id().to_owned(),
            partition_value: pv,
        }
    }
}
