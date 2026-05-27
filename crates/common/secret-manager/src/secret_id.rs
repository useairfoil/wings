use snafu::Snafu;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("secret id cannot be empty"))]
    EmptySecretId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecretId {
    raw: String,
}

impl SecretId {
    pub fn parse(raw: impl AsRef<str>) -> Result<Self, Error> {
        let raw = raw.as_ref();

        if raw.is_empty() {
            return Err(Error::EmptySecretId);
        }

        Ok(Self {
            raw: raw.to_string(),
        })
    }
}

impl AsRef<str> for SecretId {
    fn as_ref(&self) -> &str {
        &self.raw
    }
}

impl std::fmt::Display for SecretId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.raw)
    }
}

impl serde::ser::Serialize for SecretId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.raw)
    }
}

impl<'de> serde::de::Deserialize<'de> for SecretId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Self::parse(&s).map_err(serde::de::Error::custom)
    }
}
