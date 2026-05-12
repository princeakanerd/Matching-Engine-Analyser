// src/builder.rs
// Programmatic Builder for the Benchmarking Platform.
// Handles asynchronous decompression, Dockerfile generation, and image synthesis.

use crate::errors::SubmissionError;
use async_zip::tokio::read::seek::ZipFileReader;
use bollard::image::BuildImageOptions;
use bollard::Docker;
use futures_util::StreamExt;
use std::path::{Path, PathBuf};
use tokio::fs;
use tokio::io::{AsyncWriteExt, BufReader};
use tokio_tar::Builder;
use tokio_util::compat::FuturesAsyncReadCompatExt;

const BASE_DIR: &str = "/tmp/benchmarking_platform/submissions";

/// Orchestrates the full build pipeline for a submission.
pub async fn build_submission_image(submission_id: &str) -> Result<(), SubmissionError> {
    let submission_path = PathBuf::from(BASE_DIR).join(submission_id);
    let zip_file = submission_path.join("submission.zip");

    // 1. Asynchronous Decompression
    extract_submission(&zip_file, &submission_path).await?;

    // 2. Dynamic Dockerfile Generation
    generate_dockerfile(&submission_path).await?;

    // 3. Docker Build Context Serialization (Tar)
    let tar_buffer = create_tar_context(&submission_path).await?;

    // 4. Bollard Daemon Integration
    execute_docker_build(submission_id, tar_buffer).await?;

    Ok(())
}

/// Extracts the zip archive using non-blocking I/O.
async fn extract_submission(zip_path: &Path, extract_to: &Path) -> Result<(), SubmissionError> {
    let file = fs::File::open(zip_path).await?;
    let mut reader = ZipFileReader::with_tokio(BufReader::new(file)).await
        .map_err(|e| SubmissionError::BuildError(format!("Zip error: {}", e)))?;

    let entries_count = reader.file().entries().len();
    for index in 0..entries_count {
        let entry = &reader.file().entries()[index];
        let filename = entry.filename().as_str().map_err(|e| SubmissionError::BuildError(e.to_string()))?;
        let entry_path = extract_to.join(filename);

        if entry.dir().map_err(|e| SubmissionError::BuildError(e.to_string()))? {
            fs::create_dir_all(&entry_path).await?;
        } else {
            if let Some(parent) = entry_path.parent() {
                fs::create_dir_all(parent).await?;
            }
            let entry_reader = reader.reader_with_entry(index).await
                .map_err(|e| SubmissionError::BuildError(e.to_string()))?;
            let mut output_file = fs::File::create(&entry_path).await?;
            tokio::io::copy(&mut entry_reader.compat(), &mut output_file).await?;
        }
    }
    Ok(())
}

/// Generates a strict Dockerfile contract for multi-language support.
async fn generate_dockerfile(path: &Path) -> Result<(), SubmissionError> {
    let dockerfile_content = r#"
FROM ubuntu:24.04
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y \
    build-essential \
    golang \
    rustc \
    cargo \
    && rm -rf /var/lib/apt/lists/*
COPY . /app
WORKDIR /app
RUN chmod +x build.sh run.sh
RUN ./build.sh
ENTRYPOINT ["./run.sh"]
"#;
    let mut file = fs::File::create(path.join("Dockerfile")).await?;
    file.write_all(dockerfile_content.as_bytes()).await?;
    file.flush().await?;
    Ok(())
}

/// Serializes the build directory into an asynchronous tar stream.
async fn create_tar_context(path: &Path) -> Result<Vec<u8>, SubmissionError> {
    let mut tar_builder = Builder::new(Vec::new());
    tar_builder.append_dir_all(".", path).await?;
    let buffer = tar_builder.into_inner().await?;
    Ok(buffer)
}

/// Communicates with the Docker Daemon to build the linux/amd64 image.
async fn execute_docker_build(tag: &str, context: Vec<u8>) -> Result<(), SubmissionError> {
    let docker = Docker::connect_with_local_defaults()
        .map_err(|e| SubmissionError::BuildError(format!("Docker connection failed: {}", e)))?;

    let options = BuildImageOptions {
        t: format!("iicpc_submission:{}", tag),
        dockerfile: "Dockerfile".to_string(),
        platform: "linux/amd64".to_string(), // Explicit cross-platform constraint
        rm: true,
        ..Default::default()
    };

    let mut build_stream = docker.build_image(options, None, Some(context.into()));

    while let Some(result) = build_stream.next().await {
        let info = result.map_err(|e| SubmissionError::BuildError(format!("Build stream error: {}", e)))?;
        
        if let Some(error) = info.error {
            return Err(SubmissionError::BuildError(format!("Compiler Error: {}", error)));
        }

        if let Some(stream) = info.stream {
            log::info!("[Docker Build] {}", stream.trim());
        }
    }

    Ok(())
}
