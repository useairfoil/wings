use std::marker::PhantomData;

use bytes::Bytes;
use wings_txn_obj::ObjectCodec;

pub struct JsonCodec<T: Send + Sync + serde::Serialize + for<'de> serde::Deserialize<'de>> {
    _marker: std::marker::PhantomData<T>,
}

impl<T> ObjectCodec<T> for JsonCodec<T>
where
    T: Send + Sync + serde::Serialize + for<'de> serde::Deserialize<'de>,
{
    fn encode(&self, value: &T) -> Bytes {
        serde_json::to_vec(value).expect("JsonCodec failed").into()
    }

    fn decode(&self, bytes: &Bytes) -> Result<T, Box<dyn std::error::Error + Send + Sync>> {
        serde_json::from_slice(bytes)
            .map_err(|err| Box::new(err) as Box<dyn std::error::Error + Send + Sync + 'static>)
    }
}

impl<T: Send + Sync + serde::Serialize + for<'de> serde::Deserialize<'de>> Default
    for JsonCodec<T>
{
    fn default() -> Self {
        Self {
            _marker: PhantomData::default(),
        }
    }
}
