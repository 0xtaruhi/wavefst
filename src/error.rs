use std::fmt;
use std::io;

/// Convenient alias for results produced by this crate.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Errors that can be produced while reading or writing FST data.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Wrapper around standard I/O errors.
    #[error("i/o error: {0}")]
    Io(#[from] io::Error),

    /// Input data was not valid according to the FST specification.
    #[error("invalid data: {0}")]
    InvalidData(String),

    /// Encountered an unexpected or unsupported block or feature.
    #[error("unsupported feature: {0}")]
    Unsupported(String),

    /// A generic decoding failure.
    #[error("decode error: {0}")]
    Decode(String),
}

impl Error {
    pub(crate) fn invalid<T: fmt::Display>(msg: T) -> Self {
        Self::InvalidData(msg.to_string())
    }

    pub(crate) fn unsupported<T: fmt::Display>(msg: T) -> Self {
        Self::Unsupported(msg.to_string())
    }

    pub(crate) fn decode<T: fmt::Display>(msg: T) -> Self {
        Self::Decode(msg.to_string())
    }
}
