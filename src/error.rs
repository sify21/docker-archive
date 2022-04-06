use thiserror::Error;

#[derive(Error, Debug)]
pub enum DockerArchiveError {
    #[error("io error: {0:?}")]
    IOError(#[from] std::io::Error),
    #[error["invalid docker archive: {0}"]]
    InvalidArchive(&'static str),
    #[error["invalid docker archive: {0}"]]
    InvalidArchiveX(String),
    #[error("serde_json error: {0:?}")]
    DeJson(#[from] serde_json::Error),
    #[error("inter code logic error: {0:?}")]
    InternalLogicError(String),
    #[error("docker/podman executable not found: {0:?}")]
    WhichError(#[from] which::Error),
    #[error("fail to read stdout from docker/podman child process")]
    StdoutError,
}
