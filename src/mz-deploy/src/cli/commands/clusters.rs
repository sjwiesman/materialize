// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Clusters apply command - converge live cluster state to match definitions.

use crate::cli::CliError;
use crate::cli::commands::grants;
use crate::cli::executor::{
    ApplyPlan, ApplyResult, DeploymentExecutor, ObjectAction, ObjectResult, connect_apply_client,
};
use crate::client::auto_scaling::strategy_to_cluster_option;
use crate::client::{Client, Cluster, ConnectionError, quote_identifier};
use crate::config::Settings;
use crate::project::clusters::{
    self, ClusterDefinition, extract_auto_scaling_strategy, extract_replication_factor,
    extract_size,
};
use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::ast::{ClusterOption, ClusterOptionName, CreateClusterStatement, Raw};

/// Plan cluster changes without executing or printing.
pub async fn plan(
    settings: &Settings,
    client: &Client,
    executor: &DeploymentExecutor<'_>,
) -> Result<ApplyResult, CliError> {
    let profile = settings.connection();
    let directory = &settings.directory;

    let definitions = clusters::load_clusters(
        directory,
        &profile.name,
        settings.profile_suffix(),
        settings.variables(),
    )?;

    if definitions.is_empty() {
        return Ok(ApplyResult {
            phase: "clusters".to_string(),
            results: vec![],
        });
    }

    let mut object_results = Vec::new();
    for def in &definitions {
        let obj_result = plan_cluster(client, executor, def).await?;
        object_results.push(obj_result);
    }

    Ok(ApplyResult {
        phase: "clusters".to_string(),
        results: object_results,
    })
}

/// Run the `clusters apply` command: plan, render, optionally execute.
pub async fn run(settings: &Settings, dry_run: bool) -> Result<ApplyPlan, CliError> {
    let client = connect_apply_client(settings).await?;
    let executor = DeploymentExecutor::new_dry_run(&client);
    let mut plan_result = ApplyPlan::new();
    let phase = plan(settings, &client, &executor).await?;
    plan_result.add_phase(phase);

    if !dry_run {
        plan_result.execute(&client).await?;
    }

    Ok(plan_result)
}

/// Plan a single cluster definition: create if missing, alter if drifted,
/// then plan grants, revocations, and comments.
async fn plan_cluster(
    client: &Client,
    executor: &DeploymentExecutor<'_>,
    def: &ClusterDefinition,
) -> Result<ObjectResult, CliError> {
    let cluster_name = &def.name;

    // Drain any prior statements
    executor.take_statements();

    // Check if cluster already exists
    let existing = client
        .introspection()
        .get_cluster(cluster_name)
        .await
        .map_err(CliError::Connection)?;

    let action = match existing {
        None => {
            executor.execute_sql(&def.create_stmt).await?;
            ObjectAction::Created
        }
        Some(existing_cluster) => {
            // Policy drift is only diffable when the region supports autoscaling
            // strategies. On older regions the live policy is unknowable, so
            // reconciliation leaves it alone.
            let supports_auto_scaling = client.supports_auto_scaling_strategies().await?;
            let (to_set, to_reset) =
                diff_cluster_options(def, &existing_cluster, supports_auto_scaling).map_err(
                    |reason| {
                        CliError::Connection(ConnectionError::Message(format!(
                            "invalid AUTO SCALING STRATEGY for cluster '{}': {}",
                            cluster_name, reason
                        )))
                    },
                )?;

            if to_set.is_empty() && to_reset.is_empty() {
                ObjectAction::UpToDate
            } else {
                if !to_set.is_empty() {
                    let options = to_set
                        .iter()
                        .map(AstDisplay::to_ast_string_simple)
                        .collect::<Vec<_>>()
                        .join(", ");
                    let alter_sql = format!(
                        "ALTER CLUSTER {} SET ({})",
                        quote_identifier(cluster_name),
                        options
                    );
                    executor.execute_sql(&alter_sql).await?;
                }
                // SET and RESET cannot be combined in one statement.
                if !to_reset.is_empty() {
                    let names = to_reset
                        .iter()
                        .map(AstDisplay::to_ast_string_simple)
                        .collect::<Vec<_>>()
                        .join(", ");
                    let reset_sql = format!(
                        "ALTER CLUSTER {} RESET ({})",
                        quote_identifier(cluster_name),
                        names
                    );
                    executor.execute_sql(&reset_sql).await?;
                }
                ObjectAction::Altered
            }
        }
    };

    // Reconcile grants
    grants::reconcile_named_object(
        client,
        executor,
        cluster_name,
        &def.grants,
        &grants::GrantNamedObjectKind::Cluster,
    )
    .await?;

    // Execute COMMENT statements
    for comment in &def.comments {
        executor.execute_sql(comment).await?;
    }

    Ok(ObjectResult {
        object: cluster_name.clone(),
        action,
        statements: executor.take_statements(),
        redacted_statements: vec![],
        transaction_group: None,
        post_statements: vec![],
    })
}

/// The managed cluster options mz-deploy reconciles: `SIZE`, `REPLICATION
/// FACTOR`, and `AUTO SCALING STRATEGY`. Compares the definition against the
/// live cluster and returns the options to `SET` and the option names to
/// `RESET` so the caller can converge live state onto the file.
///
/// `SIZE` and `REPLICATION FACTOR` reconcile the values the file declares. `AUTO
/// SCALING STRATEGY` additionally reconciles removal, a policy dropped from the
/// file is reset, but only when `supports_auto_scaling` is set, since on older
/// regions the live policy is unknowable.
fn diff_cluster_options(
    def: &ClusterDefinition,
    existing: &Cluster,
    supports_auto_scaling: bool,
) -> Result<(Vec<ClusterOption<Raw>>, Vec<ClusterOptionName>), String> {
    let create = &def.create_stmt;
    let mut to_set = Vec::new();
    let mut to_reset = Vec::new();

    if extract_size(create).as_deref() != existing.size.as_deref()
        && let Some(option) = find_cluster_option(create, ClusterOptionName::Size)
    {
        to_set.push(option.clone());
    }
    if extract_replication_factor(create).map(i64::from) != existing.replication_factor
        && let Some(option) = find_cluster_option(create, ClusterOptionName::ReplicationFactor)
    {
        to_set.push(option.clone());
    }

    if supports_auto_scaling {
        let desired = extract_auto_scaling_strategy(create)?;
        if desired != existing.auto_scaling_strategy {
            match desired {
                Some(strategy) => to_set.push(strategy_to_cluster_option(&strategy)),
                None => to_reset.push(ClusterOptionName::AutoScalingStrategy),
            }
        }
    }

    Ok((to_set, to_reset))
}

/// Find a `CREATE CLUSTER` option by name.
fn find_cluster_option(
    create: &CreateClusterStatement<Raw>,
    name: ClusterOptionName,
) -> Option<&ClusterOption<Raw>> {
    create.options.iter().find(|option| option.name == name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_sql_parser::ast::Statement;
    use mz_sql_parser::parser::parse_statements;

    fn definition(sql: &str) -> ClusterDefinition {
        let create_stmt = match parse_statements(sql).unwrap().pop().unwrap().ast {
            Statement::CreateCluster(stmt) => stmt,
            other => panic!("expected CREATE CLUSTER, got {:?}", other),
        };
        ClusterDefinition {
            name: create_stmt.name.to_string(),
            create_stmt,
            grants: vec![],
            comments: vec![],
        }
    }

    fn cluster(size: &str, replication_factor: i64) -> Cluster {
        Cluster {
            id: "u1".to_string(),
            name: "scaled".to_string(),
            size: Some(size.to_string()),
            replication_factor: Some(replication_factor),
            auto_scaling_strategy: None,
        }
    }

    /// Render a diff as `(SET statement parts, RESET names)` for concise asserts.
    fn diff(def: &ClusterDefinition, existing: &Cluster) -> (Vec<String>, Vec<String>) {
        let (to_set, to_reset) = diff_cluster_options(def, existing, true).unwrap();
        (
            to_set
                .iter()
                .map(AstDisplay::to_ast_string_simple)
                .collect(),
            to_reset
                .iter()
                .map(AstDisplay::to_ast_string_simple)
                .collect(),
        )
    }

    #[mz_ore::test]
    fn test_diff_up_to_date() {
        let def = definition(
            "CREATE CLUSTER scaled (SIZE = '25cc', REPLICATION FACTOR = 2, \
             AUTO SCALING STRATEGY = (ON HYDRATION (HYDRATION SIZE = '100cc')))",
        );
        let mut existing = cluster("25cc", 2);
        existing.auto_scaling_strategy = extract_auto_scaling_strategy(&def.create_stmt).unwrap();
        assert_eq!(diff(&def, &existing), (vec![], Vec::<String>::new()));
    }

    #[mz_ore::test]
    fn test_diff_size_only() {
        let def = definition("CREATE CLUSTER scaled (SIZE = '50cc', REPLICATION FACTOR = 2)");
        assert_eq!(
            diff(&def, &cluster("25cc", 2)),
            (vec!["SIZE = '50cc'".to_string()], Vec::<String>::new())
        );
    }

    #[mz_ore::test]
    fn test_diff_strategy_set() {
        let def = definition(
            "CREATE CLUSTER scaled (SIZE = '25cc', REPLICATION FACTOR = 2, \
             AUTO SCALING STRATEGY = (ON HYDRATION (HYDRATION SIZE = '100cc')))",
        );
        assert_eq!(
            diff(&def, &cluster("25cc", 2)),
            (
                vec![
                    "AUTO SCALING STRATEGY = (ON HYDRATION (HYDRATION SIZE = '100cc'))".to_string()
                ],
                Vec::<String>::new()
            )
        );
    }

    #[mz_ore::test]
    fn test_diff_strategy_reset() {
        let def = definition("CREATE CLUSTER scaled (SIZE = '25cc', REPLICATION FACTOR = 2)");
        let mut existing = cluster("25cc", 2);
        existing.auto_scaling_strategy = extract_auto_scaling_strategy(
            &definition(
                "CREATE CLUSTER scaled (SIZE = '25cc', AUTO SCALING STRATEGY = \
                 (ON HYDRATION (HYDRATION SIZE = '100cc')))",
            )
            .create_stmt,
        )
        .unwrap();
        assert_eq!(
            diff(&def, &existing),
            (vec![], vec!["AUTO SCALING STRATEGY".to_string()])
        );
    }

    #[mz_ore::test]
    fn test_diff_unsupported_region_skips_strategy() {
        // With autoscaling unsupported, a live policy is never diffed even
        // though the file drops it.
        let def = definition("CREATE CLUSTER scaled (SIZE = '25cc', REPLICATION FACTOR = 2)");
        let existing = cluster("25cc", 2);
        let (to_set, to_reset) = diff_cluster_options(&def, &existing, false).unwrap();
        assert!(to_set.is_empty() && to_reset.is_empty());
    }
}
