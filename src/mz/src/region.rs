// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Display;
use std::str::FromStr;

use anyhow::{bail, ensure, Context, Result};
use futures::future::TryJoinAll;
use reqwest::{Client, Error};
use serde::de::{Unexpected, Visitor};
use serde::{Deserialize, Serialize};

use crate::configuration::ValidProfile;
use crate::utils::RequestBuilderExt;
use crate::{CloudProvider, Environment, Region};

/// Cloud providers and regions available.
#[derive(Debug, Clone, Copy)]
pub enum CloudProviderRegion {
    AwsUsEast1,
    AwsEuWest1,
}

/// Implementation to name the possible values and parse every option.
impl CloudProviderRegion {
    pub fn variants() -> [&'static str; 2] {
        ["aws/us-east-1", "aws/eu-west-1"]
    }

    /// Return the region name inside a cloud provider.
    pub fn region_name(self) -> &'static str {
        match self {
            CloudProviderRegion::AwsUsEast1 => "us-east-1",
            CloudProviderRegion::AwsEuWest1 => "eu-west-1",
        }
    }
}

impl Display for CloudProviderRegion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CloudProviderRegion::AwsUsEast1 => write!(f, "aws/us-east-1"),
            CloudProviderRegion::AwsEuWest1 => write!(f, "aws/eu-west-1"),
        }
    }
}

impl FromStr for CloudProviderRegion {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "aws/us-east-1" => Ok(CloudProviderRegion::AwsUsEast1),
            "aws/eu-west-1" => Ok(CloudProviderRegion::AwsEuWest1),
            _ => bail!("Unknown region {}", s),
        }
    }
}

impl TryFrom<&CloudProvider> for CloudProviderRegion {
    type Error = anyhow::Error;

    fn try_from(value: &CloudProvider) -> Result<Self, Self::Error> {
        format!("{}/{}", value.provider, value.region)
            .as_str()
            .parse()
    }
}

struct CloudProviderRegionVisitor;

impl<'de> Visitor<'de> for CloudProviderRegionVisitor {
    type Value = CloudProviderRegion;

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        CloudProviderRegion::from_str(v).map_err(|_| {
            E::invalid_value(
                Unexpected::Str(v),
                &format!("{:?}", CloudProviderRegion::variants()).as_str(),
            )
        })
    }

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "{:?}", CloudProviderRegion::variants())
    }
}

impl<'de> Deserialize<'de> for CloudProviderRegion {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(CloudProviderRegionVisitor)
    }
}

impl Serialize for CloudProviderRegion {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

/// Enables a particular cloud provider's region
pub(crate) async fn enable_region_environment(
    client: &Client,
    cloud_provider: &CloudProvider,
    version: Option<String>,
    environmentd_extra_args: Vec<String>,
    valid_profile: &ValidProfile<'_>,
) -> Result<Region, reqwest::Error> {
    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct Body {
        #[serde(skip_serializing_if = "Option::is_none")]
        environmentd_image_ref: Option<String>,
        #[serde(skip_serializing_if = "Vec::is_empty")]
        environmentd_extra_args: Vec<String>,
    }

    let body = Body {
        environmentd_image_ref: version.map(|v| match v.split_once(':') {
            None => format!("materialize/environmentd:{v}"),
            Some((user, v)) => format!("{user}/environmentd:{v}"),
        }),
        environmentd_extra_args,
    };

    client
        .post(
            format!(
                "{:}/api/environmentassignment",
                cloud_provider.region_controller_url
            )
            .as_str(),
        )
        .authenticate(&valid_profile.frontegg_auth)
        .json(&body)
        .send()
        .await?
        .json::<Region>()
        .await
}

/// Disables a particular cloud provider's region.
pub(crate) async fn disable_region_environment(
    client: &Client,
    cloud_provider: &CloudProvider,
    valid_profile: &ValidProfile<'_>,
) -> Result<(), reqwest::Error> {
    client
        .delete(
            format!(
                "{:}/api/environmentassignment",
                cloud_provider.region_controller_url
            )
            .as_str(),
        )
        .authenticate(&valid_profile.frontegg_auth)
        .send()
        .await?
        .error_for_status()?;
    Ok(())
}

//// Get a cloud provider's regions
pub(crate) async fn get_cloud_provider_region_details(
    client: &Client,
    cloud_provider_region: &CloudProvider,
    valid_profile: &ValidProfile<'_>,
) -> Result<Vec<Region>, anyhow::Error> {
    let mut region_api_url = cloud_provider_region.region_controller_url.clone();
    region_api_url.push_str("/api/environmentassignment");

    let response = client
        .get(region_api_url)
        .authenticate(&valid_profile.frontegg_auth)
        .send()
        .await?;
    ensure!(response.status().is_success());
    Ok(response.json::<Vec<Region>>().await?)
}

//// Get a cloud provider's region's environment
pub(crate) async fn region_environment_details(
    client: &Client,
    region: &Region,
    valid_profile: &ValidProfile<'_>,
) -> Result<Option<Vec<Environment>>, Error> {
    let mut region_api_url = region.environment_controller_url
        [0..region.environment_controller_url.len() - 4]
        .to_string();

    region_api_url.push_str("/api/environment");

    let response = client
        .get(region_api_url)
        .authenticate(&valid_profile.frontegg_auth)
        .send()
        .await?;
    match response.content_length() {
        Some(length) => {
            if length > 0 {
                Ok(Some(response.json::<Vec<Environment>>().await?))
            } else {
                Ok(None)
            }
        }
        None => Ok(None),
    }
}

pub struct CloudProviderAndRegion {
    pub cloud_provider: CloudProviderRegion,
    pub region: Option<Region>,
}

impl CloudProviderAndRegion {
    pub fn is_enabled(&self) -> bool {
        self.region.is_some()
    }
}

impl Display for CloudProviderAndRegion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let enabled = if self.is_enabled() {
            "enabled"
        } else {
            "disabled"
        };
        write!(f, "{} {}", self.cloud_provider, enabled)
    }
}

/// List all the available regions for a list of cloud providers.
pub(crate) async fn list_regions(
    client: &Client,
    valid_profile: &ValidProfile<'_>,
) -> Result<Vec<CloudProviderAndRegion>> {
    list_cloud_providers(client, valid_profile)
        .await
        .context("failed to list cloud providers")?
        .iter()
        .map(|cloud_provider| async {
            get_region_status(client, valid_profile, cloud_provider).await
        })
        .collect::<TryJoinAll<_>>()
        .await
}

async fn get_region_status(
    client: &Client,
    valid_profile: &ValidProfile<'_>,
    cloud_provider: &CloudProvider,
) -> Result<CloudProviderAndRegion> {
    let region = get_cloud_provider_region_details(client, cloud_provider, valid_profile)
        .await
        .context("failed to retrieve environment details")?
        .pop();

    Ok(CloudProviderAndRegion {
        cloud_provider: cloud_provider.try_into()?,
        region,
    })
}

/// List all the available cloud providers.
///
/// E.g.: [us-east-1, eu-west-1]
async fn list_cloud_providers(
    client: &Client,
    valid_profile: &ValidProfile<'_>,
) -> Result<Vec<CloudProvider>, Error> {
    client
        .get(valid_profile.profile.endpoint().cloud_regions_url())
        .authenticate(&valid_profile.frontegg_auth)
        .send()
        .await?
        .json::<Vec<CloudProvider>>()
        .await
}

///
/// Prints an environment's status and addresses
///
/// Healthy:         {yes/no}
/// SQL address:     foo.materialize.cloud:6875
/// HTTPS address:   <https://foo.materialize.cloud>
pub(crate) fn print_environment_status(environment: Environment, health: bool) {
    if health {
        println!("Healthy:\tyes");
    } else {
        println!("Healthy:\tno");
    }
    println!(
        "SQL address: \t{}",
        &environment.environmentd_pgwire_address
            [0..environment.environmentd_pgwire_address.len() - 5]
    );
    // Remove port from url
    println!(
        "HTTPS address: \thttps://{}",
        &environment.environmentd_https_address
            [0..environment.environmentd_https_address.len() - 4]
    );
}

pub(crate) async fn get_provider_by_region_name(
    client: &Client,
    valid_profile: &ValidProfile<'_>,
    cloud_provider_region: &CloudProviderRegion,
) -> Result<CloudProvider> {
    let cloud_providers = list_cloud_providers(client, valid_profile).await?;

    // Create a vec with only one region
    let cloud_provider: CloudProvider = cloud_providers
        .into_iter()
        .find(|provider| provider.region == cloud_provider_region.region_name())
        .with_context(|| "Retriving cloud provider from list.")?;

    Ok(cloud_provider)
}

pub(crate) async fn get_provider_region(
    client: &Client,
    valid_profile: &ValidProfile<'_>,
    cloud_provider_region: &CloudProviderRegion,
) -> Result<Option<Region>> {
    let cloud_provider =
        get_provider_by_region_name(client, valid_profile, cloud_provider_region).await?;

    get_cloud_provider_region_details(client, &cloud_provider, valid_profile)
        .await
        .map(|mut details| details.pop())
        .context("failed to retrieve region details.")
}

pub(crate) async fn get_region_environment(
    client: &Client,
    valid_profile: &ValidProfile<'_>,
    region: &Region,
) -> Result<Environment> {
    let environment_details = region_environment_details(client, region, valid_profile)
        .await
        .with_context(|| "Environment unavailable")?;
    let environment_list = environment_details.with_context(|| "Environment unlisted")?;
    let environment = environment_list
        .get(0)
        .with_context(|| "Missing environment")?;

    Ok(environment.to_owned())
}

pub(crate) async fn get_provider_region_environment(
    client: &Client,
    valid_profile: &ValidProfile<'_>,
    cloud_provider_region: &CloudProviderRegion,
) -> Result<Environment> {
    let region = get_provider_region(client, valid_profile, cloud_provider_region)
        .await
        .with_context(|| "Retrieving region data.")?
        .with_context(|| format!("Region {cloud_provider_region} is not enabled"))?;

    let environment = get_region_environment(client, valid_profile, &region)
        .await
        .with_context(|| "Retrieving environment data")?;

    Ok(environment)
}
