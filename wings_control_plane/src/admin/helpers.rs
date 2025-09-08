use std::sync::Arc;

use snafu::ResultExt;

use super::{
    Admin, AdminResult, ListTopicsRequest, NamespaceName, Topic, TopicName,
    error::InvalidResourceNameSnafu,
};

pub async fn collect_namespace_topics(
    admin: &Arc<dyn Admin>,
    namespace: &NamespaceName,
) -> AdminResult<Vec<Topic>> {
    let mut page_token = None;
    let mut topics = Vec::new();
    loop {
        let mut response = admin
            .list_topics(ListTopicsRequest {
                parent: namespace.clone(),
                page_size: 100.into(),
                page_token: page_token.clone(),
            })
            .await?;
        topics.append(&mut response.topics);
        page_token = response.next_page_token;
        if page_token.is_none() {
            break;
        }
    }

    Ok(topics)
}

pub async fn collect_namespace_topics_from_ids(
    admin: &Arc<dyn Admin>,
    namespace: &NamespaceName,
    ids: &[String],
) -> AdminResult<Vec<Topic>> {
    let mut topics = Vec::new();

    for topic_id in ids {
        let topic_name = TopicName::new(topic_id, namespace.clone())
            .context(InvalidResourceNameSnafu { resource: "topic" })?;

        match admin.get_topic(topic_name).await {
            Ok(topic) => topics.push(topic),
            Err(err) => {
                if !err.is_not_found() {
                    return Err(err);
                }
            }
        }
    }

    Ok(topics)
}
