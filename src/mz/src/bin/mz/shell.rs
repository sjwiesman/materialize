// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::os::unix::process::CommandExt;
use std::process::Command;

use anyhow::{Context, Ok, Result};
use mz::api::{get_region_info_by_cloud_provider, CloudProviderRegion, RegionInfo};
use mz::configuration::ValidProfile;
use reqwest::Client;

/// The [application_name](https://www.postgresql.org/docs/current/runtime-config-logging.html#GUC-APPLICATION-NAME)
/// which gets reported to the Postgres server we're connecting to.
const PG_APPLICATION_NAME: &str = "mz_psql";

/// ----------------------------
/// Shell command
/// ----------------------------

/// Runs psql as a subprocess command
fn run_psql_shell(valid_profile: ValidProfile<'_>, region_info: &RegionInfo) -> Result<()> {
    let error = Command::new("psql")
        .arg(region_info.sql_url(&valid_profile).to_string())
        .env("PGPASSWORD", valid_profile.app_password)
        .env("PGAPPNAME", PG_APPLICATION_NAME)
        .exec();

    Err(error).context("failed to spawn psql")
}

/// Runs pg_isready to check if an region is healthy
pub(crate) fn check_region_health(
    valid_profile: &ValidProfile<'_>,
    region_info: &RegionInfo,
) -> Result<bool> {
    if !region_info.resolvable {
        return Ok(false);
    }
    let status = Command::new("pg_isready")
        .arg("-q")
        .arg("-d")
        .arg(region_info.sql_url(valid_profile).to_string())
        .env("PGPASSWORD", valid_profile.app_password.clone())
        .output()
        .context("failed to execute pg_isready")?
        .status
        .success();

    Ok(status)
}

/// Command to run a shell (psql) on a Materialize cloud instance
pub(crate) async fn shell(
    client: Client,
    valid_profile: ValidProfile<'_>,
    cloud_provider_region: CloudProviderRegion,
) -> Result<()> {
    let region = get_region_info_by_cloud_provider(&client, &valid_profile, &cloud_provider_region)
        .await
        .context("Retrieving cloud provider region.")?;

    run_psql_shell(valid_profile, &region)
}
