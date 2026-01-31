use std::sync::Arc;

use crate::resource_type;

resource_type!(Tenant, "tenants");

/// A tenant.
///
/// A tenant is the top-level resource in Wings.
/// It is roughly equivalent to a user.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tenant {
    /// The tenant name.
    pub name: TenantName,
}

pub type TenantRef = Arc<Tenant>;

impl Tenant {
    /// Create a new tenant with the given name.
    pub fn new(name: TenantName) -> Self {
        Self { name }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tenant_creation() {
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let tenant = Tenant::new(tenant_name.clone());

        assert_eq!(tenant.name, tenant_name);
        assert_eq!(tenant.name.id(), "test-tenant");
        assert_eq!(tenant.name.name(), "tenants/test-tenant");
    }
}
