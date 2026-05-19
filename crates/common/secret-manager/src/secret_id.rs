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

    pub fn to_string(&self) -> String {
        self.raw.clone()
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
