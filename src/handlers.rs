// src/handlers.rs
// Multipart stream handler for POST /api/v1/submit.
// Processes byte chunks without buffering the full file in memory.

use crate::errors::SubmissionError;
use actix_multipart::Multipart;
use actix_web::HttpResponse;
use futures_util::StreamExt as _;
use serde::Serialize;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

/// Maximum payload size: exactly 10 MiB.
const MAX_PAYLOAD_BYTES: usize = 10 * 1024 * 1024;

/// Base directory for all submissions. Works identically on Linux x86_64 and macOS ARM64.
const SUBMISSIONS_BASE_DIR: &str = "/tmp/benchmarking_platform/submissions";

/// MIME types we accept.
const MIME_ZIP: &str = "application/zip";
const MIME_GZIP: &str = "application/gzip";

/// JSON body returned on 202 Accepted.
#[derive(Serialize)]
pub struct SubmissionAccepted {
    pub submission_id: String,
    pub file_size_bytes: u64,
}

/// Primary handler: streams a multipart file upload to disk with size and MIME validation.
pub async fn submit_handler(mut payload: Multipart) -> Result<HttpResponse, SubmissionError> {
    // Iterate over multipart fields until we find the first file field.
    while let Some(field_result) = payload.next().await {
        let mut field = field_result.map_err(|e| SubmissionError::MultipartError(e.to_string()))?;

        // ── MIME validation ──────────────────────────────────────────────
        let content_type = field.content_type().map(|ct| ct.to_string());

        let mime_str = match &content_type {
            Some(ct) => ct.as_str(),
            None => {
                return Err(SubmissionError::InvalidMimeType(
                    "missing Content-Type".to_string(),
                ));
            }
        };

        if mime_str != MIME_ZIP && mime_str != MIME_GZIP {
            return Err(SubmissionError::InvalidMimeType(mime_str.to_string()));
        }

        // ── Determine file extension from validated MIME ─────────────────
        let extension = if mime_str == MIME_ZIP { "zip" } else { "gz" };

        // ── Generate submission ID and prepare directory ─────────────────
        let submission_id = Uuid::new_v4();
        let submission_dir =
            std::path::PathBuf::from(SUBMISSIONS_BASE_DIR).join(submission_id.to_string());

        fs::create_dir_all(&submission_dir).await?;

        let file_path = submission_dir.join(format!("submission.{}", extension));

        // ── Stream chunks to disk, enforcing size limit ─────────────────
        let mut file = fs::File::create(&file_path).await?;
        let mut total_bytes_written: u64 = 0;

        while let Some(chunk_result) = field.next().await {
            let chunk =
                chunk_result.map_err(|e| SubmissionError::MultipartError(e.to_string()))?;

            let new_total = total_bytes_written + chunk.len() as u64;
            if new_total > MAX_PAYLOAD_BYTES as u64 {
                // Abort: close the file handle, remove the partial submission directory,
                // then return the 413 error.
                drop(file);
                let _ = fs::remove_dir_all(&submission_dir).await;
                return Err(SubmissionError::PayloadTooLarge);
            }

            file.write_all(&chunk).await?;
            total_bytes_written = new_total;
        }

        // Ensure all buffered data is flushed to the OS.
        file.flush().await?;
        file.shutdown().await?;

        log::info!(
            "Submission {} accepted: {} bytes written to {}",
            submission_id,
            total_bytes_written,
            file_path.display()
        );

        // ── 202 Accepted ────────────────────────────────────────────────
        let response_body = SubmissionAccepted {
            submission_id: submission_id.to_string(),
            file_size_bytes: total_bytes_written,
        };

        // ── Trigger Step 2 & 3: Programmatic Builder & Orchestrator ──────
        let submission_id_str = submission_id.to_string();
        tokio::spawn(async move {
            if let Err(e) = crate::builder::build_submission_image(&submission_id_str).await {
                log::error!("Build failed for submission {}: {}", submission_id_str, e);
            } else {
                log::info!("Build successful for submission {}. Starting orchestration...", submission_id_str);
                if let Err(e) = crate::orchestrator::run_submission_container(&submission_id_str).await {
                    log::error!("Execution failed for submission {}: {}", submission_id_str, e);
                }
            }
        });

        return Ok(HttpResponse::Accepted().json(response_body));
    }

    // If we exit the loop without finding a file field, the payload was empty.
    Err(SubmissionError::NoFileField)
}
