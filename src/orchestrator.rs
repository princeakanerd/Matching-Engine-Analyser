// src/orchestrator.rs
// Orchestration Engine for the Benchmarking Platform.
// Handles container instantiation, resource capping, and execution monitoring.

use crate::errors::SubmissionError;
use bollard::container::{Config, CreateContainerOptions, StartContainerOptions, WaitContainerOptions};
use bollard::models::{Resources, HostConfig};
use bollard::Docker;
use futures_util::StreamExt;
use std::default::Default;

/// Orchestrates the execution of a built submission image.
pub async fn run_submission_container(submission_id: &str) -> Result<(), SubmissionError> {
    let docker = Docker::connect_with_local_defaults()
        .map_err(|e| SubmissionError::BuildError(format!("Docker connection failed: {}", e)))?;

    let image_name = format!("iicpc_submission:{}", submission_id);
    let container_name = format!("run_{}", submission_id);

    // 1. Define Resource Constraints (1 CPU, 512MB RAM)
    let host_config = HostConfig {
        cpu_quota: Some(100000), // 100,000 microseconds per 100,000 microsecond period = 1 CPU
        cpu_period: Some(100000),
        memory: Some(512 * 1024 * 1024), // 512 MB
        auto_remove: Some(true), // Automatically delete container after exit
        ..Default::default()
    };

    // 2. Configure the Container
    let config = Config {
        image: Some(image_name),
        host_config: Some(host_config),
        attach_stdout: Some(true),
        attach_stderr: Some(true),
        ..Default::default()
    };

    // 3. Create the Container
    log::info!("[Orchestrator] Creating container for submission: {}", submission_id);
    docker.create_container(
        Some(CreateContainerOptions {
            name: container_name.clone(),
            ..Default::default()
        }),
        config,
    ).await.map_err(|e| SubmissionError::BuildError(format!("Container creation failed: {}", e)))?;

    // 4. Start the Container
    log::info!("[Orchestrator] Starting execution...");
    docker.start_container(&container_name, None::<StartContainerOptions<String>>)
        .await
        .map_err(|e| SubmissionError::BuildError(format!("Container start failed: {}", e)))?;

    // 5. Stream and Log Output (Telemetry)
    let mut logs = docker.logs(
        &container_name,
        Some(bollard::container::LogsOptions::<String> {
            follow: true,
            stdout: true,
            stderr: true,
            ..Default::default()
        }),
    );

    while let Some(log_result) = logs.next().await {
        match log_result {
            Ok(output) => log::info!("[Engine Log] {}", output.to_string().trim()),
            Err(e) => log::error!("[Orchestrator Error] Failed to read log: {}", e),
        }
    }

    // 6. Wait for Completion
    let mut wait_stream = docker.wait_container(
        &container_name,
        Some(WaitContainerOptions {
            condition: "not-running",
        }),
    );

    if let Some(wait_result) = wait_stream.next().await {
        match wait_result {
            Ok(response) => {
                log::info!("[Orchestrator] Container exited with status code: {}", response.status_code);
            }
            Err(e) => log::error!("[Orchestrator] Error waiting for container: {}", e),
        }
    }

    Ok(())
}
