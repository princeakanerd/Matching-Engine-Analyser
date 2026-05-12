// src/errors.rs
// Custom error types implementing actix_web::ResponseError for structured JSON error responses.

use actix_web::{http::StatusCode, HttpResponse, ResponseError};
use serde::Serialize;
use std::fmt;

/// Structured JSON error body returned to clients on failure.
#[derive(Serialize)]
struct ErrorResponse {
    error_code: u16,
    message: String,
}

/// All possible failure states during submission processing.
#[derive(Debug)]
pub enum SubmissionError {
    /// Filesystem I/O failure (directory creation, file write, flush).
    IoError(std::io::Error),
    /// Incoming payload exceeds the 10 MiB ceiling.
    PayloadTooLarge,
    /// The uploaded file's MIME type is not application/zip or application/gzip.
    InvalidMimeType(String),
    /// Multipart stream-level error (malformed boundary, truncated stream, etc.).
    MultipartError(String),
    /// Failure during Docker image building or decompression.
    BuildError(String),
    /// No file field was found in the multipart payload.
    NoFileField,
}

impl fmt::Display for SubmissionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SubmissionError::IoError(e) => {
                write!(f, "Filesystem I/O error: {}", e)
            }
            SubmissionError::PayloadTooLarge => {
                write!(
                    f,
                    "Payload exceeds maximum allowed size of {} bytes (10 MiB)",
                    10 * 1024 * 1024
                )
            }
            SubmissionError::InvalidMimeType(mime) => {
                write!(
                    f,
                    "Unsupported media type '{}'. Only application/zip and application/gzip are accepted",
                    mime
                )
            }
            SubmissionError::MultipartError(msg) => {
                write!(f, "Multipart processing error: {}", msg)
            }
            SubmissionError::BuildError(msg) => {
                write!(f, "Build system error: {}", msg)
            }
            SubmissionError::NoFileField => {
                write!(f, "No file field found in multipart payload")
            }
        }
    }
}

impl ResponseError for SubmissionError {
    fn status_code(&self) -> StatusCode {
        match self {
            SubmissionError::IoError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            SubmissionError::PayloadTooLarge => StatusCode::PAYLOAD_TOO_LARGE,
            SubmissionError::InvalidMimeType(_) => StatusCode::UNSUPPORTED_MEDIA_TYPE,
            SubmissionError::MultipartError(_) => StatusCode::BAD_REQUEST,
            SubmissionError::BuildError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            SubmissionError::NoFileField => StatusCode::BAD_REQUEST,
        }
    }

    fn error_response(&self) -> HttpResponse {
        let status = self.status_code();
        let body = ErrorResponse {
            error_code: status.as_u16(),
            message: self.to_string(),
        };
        HttpResponse::build(status).json(body)
    }
}

// Seamless conversion from std::io::Error via the ? operator.
impl From<std::io::Error> for SubmissionError {
    fn from(err: std::io::Error) -> Self {
        SubmissionError::IoError(err)
    }
}
