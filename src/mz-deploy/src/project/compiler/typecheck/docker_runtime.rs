//! Docker runtime for compiler-owned runtime typechecking.
//!
//! This module manages a persistent Materialize container and returns a
//! connected client. Dependency staging is handled by the typecheck executor,
//! which creates only the temporary objects required for the current dirty
//! frontier.

use super::TypeCheckError;
use crate::client::{Client, Profile};
use crate::config::default_docker_image;
use crate::{timing, verbose};
use tokio::process::Command;
use tokio::time::{Duration, sleep};

/// Possible states of Docker availability on the host system.
pub enum DockerStatus {
    Running,
    NotRunning,
    NotInstalled,
}

/// Name of the persistent Docker container.
const CONTAINER_NAME: &str = "mz-deploy-typecheck";

/// Host port to bind for the persistent container.
const CONTAINER_PORT: u16 = 16875;

/// Manages the Materialize container used for runtime validation.
pub struct DockerRuntime {
    image: String,
}

impl DockerRuntime {
    pub async fn check_availability() -> DockerStatus {
        let result = Command::new("docker")
            .arg("info")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .await;

        match result {
            Ok(status) if status.success() => DockerStatus::Running,
            Ok(_) => DockerStatus::NotRunning,
            Err(_) => DockerStatus::NotInstalled,
        }
    }

    pub fn new() -> Self {
        Self {
            image: default_docker_image(),
        }
    }

    pub fn with_image(mut self, image: impl Into<String>) -> Self {
        self.image = image.into();
        self
    }

    pub async fn get_client(&self) -> Result<Client, TypeCheckError> {
        let start = std::time::Instant::now();

        let profile = Self::make_profile();
        let client = match Client::connect_with_profile(profile).await {
            Ok(client) => {
                timing!("  connect (fast-path)", start.elapsed());
                verbose!(
                    "Fast-path connect succeeded ({}ms)",
                    start.elapsed().as_millis()
                );
                client
            }
            Err(_) => {
                timing!("  connect (fast-path fail)", start.elapsed());
                verbose!(
                    "Fast-path connect failed ({}ms), falling back to Docker CLI",
                    start.elapsed().as_millis()
                );
                let ensure_start = std::time::Instant::now();
                self.ensure_container().await?;
                timing!("  ensure_container", ensure_start.elapsed());

                let connect_start = std::time::Instant::now();
                let profile = Self::make_profile();
                verbose!("Connecting to Materialize...");
                let client = Client::connect_with_profile(profile).await?;
                timing!("  connect (slow-path)", connect_start.elapsed());
                verbose!("Connected ({}ms)", connect_start.elapsed().as_millis());
                client
            }
        };

        verbose!("get_client total ({}ms)", start.elapsed().as_millis());
        Ok(client)
    }

    fn make_profile() -> Profile {
        Profile {
            name: "docker-typecheck".to_string(),
            host: "localhost".to_string(),
            port: CONTAINER_PORT,
            username: "materialize".to_string(),
            password: None,
            options: Default::default(),
        }
    }

    async fn container_exists(&self) -> Result<bool, TypeCheckError> {
        let output = Command::new("docker")
            .args([
                "ps",
                "-a",
                "--filter",
                &format!("name=^{}$", CONTAINER_NAME),
                "--format",
                "{{.Names}}",
            ])
            .output()
            .await
            .map_err(|e| TypeCheckError::ContainerStartFailed(Box::new(e)))?;

        Ok(output.status.success() && !output.stdout.is_empty())
    }

    async fn container_is_running(&self) -> Result<bool, TypeCheckError> {
        let output = Command::new("docker")
            .args([
                "ps",
                "--filter",
                &format!("name=^{}$", CONTAINER_NAME),
                "--format",
                "{{.Names}}",
            ])
            .output()
            .await
            .map_err(|e| TypeCheckError::ContainerStartFailed(Box::new(e)))?;

        Ok(output.status.success() && !output.stdout.is_empty())
    }

    async fn container_is_healthy(&self) -> bool {
        Client::connect_with_profile(Self::make_profile())
            .await
            .is_ok()
    }

    async fn remove_container(&self) -> Result<(), TypeCheckError> {
        verbose!("Removing existing container: {}", CONTAINER_NAME);
        let output = Command::new("docker")
            .args(["rm", "-f", CONTAINER_NAME])
            .output()
            .await
            .map_err(|e| TypeCheckError::ContainerStartFailed(Box::new(e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(TypeCheckError::ContainerStartFailed(
                format!("Failed to remove container: {}", stderr).into(),
            ));
        }
        Ok(())
    }

    async fn create_container(&self) -> Result<(), TypeCheckError> {
        verbose!(
            "Creating persistent container: {} (image: {})",
            CONTAINER_NAME,
            self.image
        );

        let output = Command::new("docker")
            .args([
                "run",
                "-d",
                "--name",
                CONTAINER_NAME,
                "-e",
                "MZ_EAT_MY_DATA=1",
                "-p",
                &format!("{}:6875", CONTAINER_PORT),
                &self.image,
                "--system-parameter-default=max_tables=10000",
                "--system-parameter-default=max_objects_per_schema=10000",
            ])
            .output()
            .await
            .map_err(|e| TypeCheckError::ContainerStartFailed(Box::new(e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(TypeCheckError::ContainerStartFailed(
                format!("Failed to create container: {}", stderr).into(),
            ));
        }
        Ok(())
    }

    async fn start_existing_container(&self) -> Result<(), TypeCheckError> {
        verbose!("Starting existing container: {}", CONTAINER_NAME);

        let output = Command::new("docker")
            .args(["start", CONTAINER_NAME])
            .output()
            .await
            .map_err(|e| TypeCheckError::ContainerStartFailed(Box::new(e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(TypeCheckError::ContainerStartFailed(
                format!("Failed to start container: {}", stderr).into(),
            ));
        }
        Ok(())
    }

    async fn wait_for_container(&self) -> Result<(), TypeCheckError> {
        verbose!("Waiting for container to be ready...");
        for i in 0..30 {
            if self.container_is_healthy().await {
                verbose!("Container is ready!");
                return Ok(());
            }
            if i < 29 {
                sleep(Duration::from_secs(1)).await;
            }
        }
        Err(TypeCheckError::ContainerStartFailed(
            "Container failed to become healthy within 30 seconds".into(),
        ))
    }

    async fn ensure_container(&self) -> Result<(), TypeCheckError> {
        let exists = self.container_exists().await?;
        let is_running = if exists {
            self.container_is_running().await?
        } else {
            false
        };

        if is_running {
            verbose!("Found running container: {}", CONTAINER_NAME);
            if self.container_is_healthy().await {
                verbose!("Container is healthy, reusing it");
                return Ok(());
            } else {
                verbose!("Container is unhealthy, recreating it");
                self.remove_container().await?;
            }
        } else if exists {
            verbose!("Found stopped container: {}", CONTAINER_NAME);
            match self.start_existing_container().await {
                Ok(_) => {
                    self.wait_for_container().await?;
                    return Ok(());
                }
                Err(_) => {
                    verbose!("Failed to start stopped container, recreating it");
                    self.remove_container().await?;
                }
            }
        }

        self.create_container().await?;
        self.wait_for_container().await?;
        Ok(())
    }
}

impl Default for DockerRuntime {
    fn default() -> Self {
        Self::new()
    }
}
