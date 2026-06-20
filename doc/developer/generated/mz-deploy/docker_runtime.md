---
source: src/mz-deploy/src/docker_runtime.rs
revision: a647094cc4
---

# mz_deploy::docker_runtime

Manages a persistent local `materialize/materialized` Docker container used by the
`test` command to run unit tests against a real Materialize instance.
`DockerRuntime` holds the image name (default from `config::default_docker_image`,
overridable via `with_image`). `check_availability` shells out to `docker info` to
report a `DockerStatus` (`Running`, `NotRunning`, `NotInstalled`).

`get_client` first attempts a fast-path connection (`connect_with_profile_no_pin`,
which bypasses the usual session-cluster pin since the ephemeral container has no
`_mz_deploy_server` cluster); on failure it ensures the container is up via
`ensure_container` and reconnects. `ensure_container` inspects the named container
(`CONTAINER_NAME`) using `docker ps`: it reuses a healthy running container,
recreates an unhealthy one, starts a stopped one, and otherwise creates a fresh
one. The container-lifecycle helpers (`container_exists`, `container_is_running`,
`container_is_healthy`, `remove_container`, `create_container`,
`start_existing_container`, `wait_for_container`) each drive a single `docker`
subcommand; `create_container` binds `CONTAINER_PORT` to the container's 6875 and
sets data/limit parameters, and `wait_for_container` polls health for up to 30
seconds. `make_profile` builds the loopback connection `Profile`.
`DockerRuntimeError` covers container-start and connection failures.
