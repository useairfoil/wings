use wings_secret_manager::{Error, SecretId, SecretManager, azure::AzureKeyVaultBuilder};

fn get_test_vault_name() -> Option<String> {
    std::env::var("TEST_AZURE_VAULT_NAME").ok()
}

#[tokio::test]
async fn test_azure_key_vault() {
    let Some(vault_name) = get_test_vault_name() else {
        return;
    };

    let azure = AzureKeyVaultBuilder::new()
        .with_vault_name(vault_name)
        .build()
        .unwrap();

    let secret_id = SecretId::parse("my-secret").unwrap();
    let secret_value = "very secret".to_string();

    azure
        .create_secret(&secret_id, secret_value.clone())
        .await
        .unwrap();

    let secret = azure.get_secret(&secret_id).await.unwrap();
    assert_eq!(secret.value, secret_value);

    azure.delete_secret(&secret_id).await.unwrap();

    let error = azure.get_secret(&secret_id).await.unwrap_err();
    assert!(matches!(error, Error::NotFound { .. }))
}
