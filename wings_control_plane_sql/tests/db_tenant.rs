use wings_resources::TenantName;

mod common;

#[tokio::test]
async fn test_tenant_roundtrip() {
    let db = common::new_test_db().await;

    let name = TenantName::new_unchecked("abcd");
    let back = db.create_tenant(name.clone()).await.unwrap();
    assert_eq!(back.name, name);
}
