// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Catalog-backed runtime typechecking.
//!
//! Validates objects against an in-memory catalog built from `mz-sql`,
//! without requiring a running Materialize container. This backend is faster
//! and more portable than the Docker backend, at the cost of lower fidelity
//! (the in-memory catalog may not reproduce all Materialize behaviors).
//!
//! **Key invariant:** Each catalog instance is scoped to a single typecheck
//! run. A fresh catalog is created for each object's validation, populated
//! with its dependencies, then discarded. This avoids state leaking between
//! validation of unrelated objects.

use super::error::ObjectTypeCheckErrorKind;
use super::{ObjectTypeCheckError, TypeCheckError};
use crate::project::ir::object_id::ObjectId;
use crate::timing;
use crate::types::ColumnType;
use chrono::Utc;
use mz_build_info::DUMMY_BUILD_INFO;
use mz_catalog::builtin::{BUILTINS, Builtin, BuiltinType};
use mz_controller_types::{ClusterId, ReplicaId};
use mz_expr::MirScalarExpr;
use mz_ore::collections::HashMap;
use mz_ore::now::NOW_ZERO;
use mz_repr::adt::mz_acl_item::{AclMode, PrivilegeMap};
use mz_repr::explain::{DummyHumanizer, ExprHumanizer};
use mz_repr::network_policy_id::NetworkPolicyId;
use mz_repr::role_id::RoleId;
use mz_repr::{
    CatalogItemId, GlobalId, RelationDesc, RelationVersion, RelationVersionSelector, SqlScalarType,
};
use mz_secrets::InMemorySecretsController;
use mz_sql::ast::Expr;
use mz_sql::catalog::{
    BuiltinsConfig, CatalogCluster, CatalogClusterReplica, CatalogConfig, CatalogDatabase,
    CatalogError, CatalogItem, CatalogItemType, CatalogNetworkPolicy, CatalogRole, CatalogSchema,
    CatalogType, CatalogTypeDetails, DefaultPrivilegeAclItem, DefaultPrivilegeObject,
    EnvironmentId, IdReference, NameReference, ObjectType as SqlObjectType, RoleAttributes,
    SessionCatalog, SystemObjectType,
};
use mz_sql::names::{
    Aug, FullItemName, FullSchemaName, ItemQualifiers, PartialItemName, QualifiedItemName,
    QualifiedSchemaName, RawDatabaseSpecifier, ResolvedDatabaseSpecifier, ResolvedIds, SchemaId,
    SchemaSpecifier,
};
use mz_sql::plan::{ClusterSchedule, Params, Plan, PlanContext, PlanError, StatementDesc};
use mz_sql::session::user::MZ_SYSTEM_ROLE_ID;
use mz_sql::session::vars::{OwnedVarInput, SystemVars};
use mz_storage_types::connections::Connection;
use mz_storage_types::connections::inline::{
    ConnectionResolver, InlinedConnection, ReferencedConnection,
};
use mz_storage_types::sources::{SourceDesc, SourceExportDataConfig, SourceExportDetails};
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use uuid::Uuid;

const DEFAULT_CLUSTER_NAME: &str = "mz_deploy";
const FIRST_USER_OID: u32 = 50_000;

/// Counters for the catalog's various identifier namespaces.
#[derive(Debug, Clone)]
struct IdAllocator {
    database: u64,
    schema: u64,
    item: u64,
    global: u64,
    oid: u32,
}

impl IdAllocator {
    fn new() -> Self {
        Self {
            database: 1,
            schema: 1,
            item: 1,
            global: 1,
            oid: FIRST_USER_OID,
        }
    }

    fn allocate_database(&mut self) -> u64 {
        let id = self.database;
        self.database += 1;
        id
    }

    fn allocate_schema(&mut self) -> u64 {
        let id = self.schema;
        self.schema += 1;
        id
    }

    fn allocate_item(&mut self) -> u64 {
        let id = self.item;
        self.item += 1;
        id
    }

    fn allocate_global(&mut self) -> u64 {
        let id = self.global;
        self.global += 1;
        id
    }

    fn allocate_oid(&mut self) -> Result<u32, CatalogError> {
        let oid = self.oid;
        self.oid = self.oid.checked_add(1).ok_or(CatalogError::OidExhaustion)?;
        Ok(oid)
    }
}

/// User database — created on demand by `bootstrap_namespaces`.
#[derive(Debug, Clone)]
struct LocalDatabase {
    name: String,
    id: mz_sql::names::DatabaseId,
    schema_ids: BTreeMap<String, SchemaId>,
    owner_id: RoleId,
    privileges: PrivilegeMap,
}

impl CatalogDatabase for LocalDatabase {
    fn name(&self) -> &str {
        &self.name
    }

    fn id(&self) -> mz_sql::names::DatabaseId {
        self.id
    }

    fn has_schemas(&self) -> bool {
        true
    }

    fn schema_ids(&self) -> &BTreeMap<String, SchemaId> {
        &self.schema_ids
    }

    fn schemas(&self) -> Vec<&dyn CatalogSchema> {
        Vec::new()
    }

    fn owner_id(&self) -> RoleId {
        self.owner_id
    }

    fn privileges(&self) -> &PrivilegeMap {
        &self.privileges
    }
}

/// Schema (system or user). `item_ids` is mutated as items are inserted.
#[derive(Debug, Clone)]
struct LocalSchema {
    database: ResolvedDatabaseSpecifier,
    name: QualifiedSchemaName,
    id: SchemaSpecifier,
    item_ids: BTreeSet<CatalogItemId>,
    owner_id: RoleId,
    privileges: PrivilegeMap,
}

impl CatalogSchema for LocalSchema {
    fn database(&self) -> &ResolvedDatabaseSpecifier {
        &self.database
    }

    fn name(&self) -> &QualifiedSchemaName {
        &self.name
    }

    fn id(&self) -> &SchemaSpecifier {
        &self.id
    }

    fn has_items(&self) -> bool {
        !self.item_ids.is_empty()
    }

    fn item_ids(&self) -> Box<dyn Iterator<Item = CatalogItemId> + '_> {
        Box::new(self.item_ids.iter().copied())
    }

    fn owner_id(&self) -> RoleId {
        self.owner_id
    }

    fn privileges(&self) -> &PrivilegeMap {
        &self.privileges
    }
}

/// Stub role for the single-role typecheck context — only `MZ_SYSTEM_ROLE_ID` is ever active.
#[derive(Debug, Clone)]
struct StubRole {
    name: String,
    id: RoleId,
    membership: BTreeMap<RoleId, RoleId>,
    attributes: RoleAttributes,
    vars: BTreeMap<String, OwnedVarInput>,
}

impl CatalogRole for StubRole {
    fn name(&self) -> &str {
        &self.name
    }

    fn id(&self) -> RoleId {
        self.id
    }

    fn membership(&self) -> &BTreeMap<RoleId, RoleId> {
        &self.membership
    }

    fn attributes(&self) -> &RoleAttributes {
        &self.attributes
    }

    fn vars(&self) -> &BTreeMap<String, OwnedVarInput> {
        &self.vars
    }
}

/// Stub cluster — typecheck only needs one (the implicit `quickstart`-equivalent).
#[derive(Debug, Clone)]
struct StubCluster {
    name: String,
    id: ClusterId,
    bound_objects: BTreeSet<CatalogItemId>,
    replica_ids: BTreeMap<String, ReplicaId>,
    owner_id: RoleId,
    privileges: PrivilegeMap,
}

impl<'a> CatalogCluster<'a> for StubCluster {
    fn name(&self) -> &str {
        &self.name
    }

    fn id(&self) -> ClusterId {
        self.id
    }

    fn bound_objects(&self) -> &BTreeSet<CatalogItemId> {
        &self.bound_objects
    }

    fn replica_ids(&self) -> &BTreeMap<String, ReplicaId> {
        &self.replica_ids
    }

    fn replicas(&self) -> Vec<&dyn CatalogClusterReplica<'_>> {
        Vec::new()
    }

    fn replica(&self, _id: ReplicaId) -> &dyn CatalogClusterReplica<'_> {
        // Cluster replicas aren't part of project SQL — never reached during typecheck.
        unreachable!("catalog backend has no cluster replicas")
    }

    fn owner_id(&self) -> RoleId {
        self.owner_id
    }

    fn privileges(&self) -> &PrivilegeMap {
        &self.privileges
    }

    fn is_managed(&self) -> bool {
        false
    }

    fn managed_size(&self) -> Option<&str> {
        None
    }

    fn schedule(&self) -> Option<&ClusterSchedule> {
        None
    }

    fn try_to_plan(&self) -> Result<mz_sql::plan::CreateClusterPlan, PlanError> {
        Err(PlanError::Unsupported {
            feature: "clusters are not supported by the catalog typecheck backend".into(),
            discussion_no: None,
        })
    }
}

/// In-memory catalog item representing a database object (table, view,
/// function, type, etc.) satisfying the [`CatalogItem`] trait.
#[derive(Debug, Clone)]
struct LocalItem {
    name: QualifiedItemName,
    id: CatalogItemId,
    global_id: GlobalId,
    oid: u32,
    item_type: CatalogItemType,
    create_sql: String,
    references: ResolvedIds,
    uses: BTreeSet<CatalogItemId>,
    referenced_by: Vec<CatalogItemId>,
    used_by: Vec<CatalogItemId>,
    relation_desc: Option<RelationDesc>,
    func: Option<&'static mz_sql::func::Func>,
    type_details: Option<CatalogTypeDetails<IdReference>>,
    owner_id: RoleId,
    privileges: PrivilegeMap,
    cluster_id: Option<ClusterId>,
}

impl CatalogItem for LocalItem {
    fn name(&self) -> &QualifiedItemName {
        &self.name
    }

    fn id(&self) -> CatalogItemId {
        self.id
    }

    fn global_ids(&self) -> Box<dyn Iterator<Item = GlobalId> + '_> {
        Box::new(std::iter::once(self.global_id))
    }

    fn oid(&self) -> u32 {
        self.oid
    }

    fn func(&self) -> Result<&'static mz_sql::func::Func, CatalogError> {
        self.func.ok_or_else(|| CatalogError::UnexpectedType {
            name: self.name.item.clone(),
            actual_type: self.item_type,
            expected_type: CatalogItemType::Func,
        })
    }

    fn source_desc(&self) -> Result<Option<&SourceDesc<ReferencedConnection>>, CatalogError> {
        Err(CatalogError::UnexpectedType {
            name: self.name.item.clone(),
            actual_type: self.item_type,
            expected_type: CatalogItemType::Source,
        })
    }

    fn connection(&self) -> Result<Connection<ReferencedConnection>, CatalogError> {
        Err(CatalogError::UnexpectedType {
            name: self.name.item.clone(),
            actual_type: self.item_type,
            expected_type: CatalogItemType::Connection,
        })
    }

    fn item_type(&self) -> CatalogItemType {
        self.item_type
    }

    fn create_sql(&self) -> &str {
        &self.create_sql
    }

    fn references(&self) -> &ResolvedIds {
        &self.references
    }

    fn uses(&self) -> BTreeSet<CatalogItemId> {
        self.uses.clone()
    }

    fn referenced_by(&self) -> &[CatalogItemId] {
        &self.referenced_by
    }

    fn used_by(&self) -> &[CatalogItemId] {
        &self.used_by
    }

    fn subsource_details(
        &self,
    ) -> Option<(
        CatalogItemId,
        &mz_sql_parser::ast::UnresolvedItemName,
        &SourceExportDetails,
    )> {
        None
    }

    fn source_export_details(
        &self,
    ) -> Option<(
        CatalogItemId,
        &mz_sql_parser::ast::UnresolvedItemName,
        &SourceExportDetails,
        &SourceExportDataConfig<ReferencedConnection>,
    )> {
        None
    }

    fn is_progress_source(&self) -> bool {
        false
    }

    fn progress_id(&self) -> Option<CatalogItemId> {
        None
    }

    fn index_details(&self) -> Option<(&[MirScalarExpr], GlobalId)> {
        None
    }

    fn writable_table_details(&self) -> Option<&[Expr<Aug>]> {
        None
    }

    fn replacement_target(&self) -> Option<CatalogItemId> {
        None
    }

    fn type_details(&self) -> Option<&CatalogTypeDetails<IdReference>> {
        self.type_details.as_ref()
    }

    fn owner_id(&self) -> RoleId {
        self.owner_id
    }

    fn privileges(&self) -> &PrivilegeMap {
        &self.privileges
    }

    fn cluster_id(&self) -> Option<ClusterId> {
        self.cluster_id
    }

    fn at_version(
        &self,
        _version: RelationVersionSelector,
    ) -> Box<dyn mz_sql::catalog::CatalogCollectionItem> {
        Box::new(self.clone())
    }

    fn latest_version(&self) -> Option<RelationVersion> {
        None
    }
}

impl mz_sql::catalog::CatalogCollectionItem for LocalItem {
    fn relation_desc(&self) -> Option<Cow<'_, RelationDesc>> {
        self.relation_desc.as_ref().map(Cow::Borrowed)
    }

    fn global_id(&self) -> GlobalId {
        self.global_id
    }
}

/// In-memory implementation of Materialize's [`SessionCatalog`] trait.
///
/// Provides name resolution and type information for the SQL planner.
/// Pre-populated with system schemas and all builtin types, functions, and
/// system objects on creation, then incrementally extended with project
/// objects during validation. Each instance is scoped to a single typecheck
/// run to avoid state leakage between validations.
#[derive(Debug, Clone)]
pub(super) struct CatalogRuntime {
    active_role: StubRole,
    active_database: Option<mz_sql::names::DatabaseId>,
    active_cluster_name: String,
    search_path: Vec<(ResolvedDatabaseSpecifier, SchemaSpecifier)>,
    databases_by_id: BTreeMap<mz_sql::names::DatabaseId, LocalDatabase>,
    databases_by_name: BTreeMap<String, mz_sql::names::DatabaseId>,
    ambient_schemas_by_name: BTreeMap<String, SchemaSpecifier>,
    schemas_by_key: BTreeMap<(ResolvedDatabaseSpecifier, String), LocalSchema>,
    schemas_by_id: BTreeMap<(ResolvedDatabaseSpecifier, SchemaSpecifier), LocalSchema>,
    items_by_id: BTreeMap<CatalogItemId, Arc<LocalItem>>,
    items_by_global_id: BTreeMap<GlobalId, CatalogItemId>,
    /// Maps qualified names to item IDs. A `Vec` because Materialize's builtin
    /// catalog has types and functions that share the same qualified name (e.g.
    /// `pg_catalog.date` is both a type and a cast function). Lookups filter by
    /// item kind via a predicate.
    items_by_name: HashMap<QualifiedItemName, Vec<CatalogItemId>>,
    cluster: StubCluster,
    config: CatalogConfig,
    system_vars: SystemVars,
    ids: IdAllocator,
    mz_internal_schema_id: SchemaId,
    mz_unsafe_schema_id: SchemaId,
}

impl CatalogRuntime {
    /// Create a catalog pre-populated with system schemas (pg_catalog,
    /// mz_catalog, etc.) and all builtin types, functions, and system objects.
    fn new() -> Result<Self, CatalogError> {
        let active_role = StubRole {
            name: "mz_system".into(),
            id: MZ_SYSTEM_ROLE_ID,
            membership: BTreeMap::new(),
            attributes: RoleAttributes::new(),
            vars: BTreeMap::new(),
        };
        let cluster = StubCluster {
            name: DEFAULT_CLUSTER_NAME.into(),
            id: ClusterId::User(1),
            bound_objects: BTreeSet::new(),
            replica_ids: BTreeMap::new(),
            owner_id: MZ_SYSTEM_ROLE_ID,
            privileges: PrivilegeMap::default(),
        };
        let secrets_reader = Arc::new(InMemorySecretsController::new());
        let config = CatalogConfig {
            start_time: Utc::now(),
            start_instant: Instant::now(),
            nonce: 0,
            environment_id: EnvironmentId::for_tests(),
            session_id: Uuid::nil(),
            build_info: &DUMMY_BUILD_INFO,
            now: NOW_ZERO.clone(),
            connection_context: mz_storage_types::connections::ConnectionContext::for_tests(
                secrets_reader,
            ),
            builtins_cfg: BuiltinsConfig {
                include_continual_tasks: true,
            },
            helm_chart_version: None,
        };
        let mut catalog = Self {
            active_role,
            active_database: None,
            active_cluster_name: DEFAULT_CLUSTER_NAME.into(),
            search_path: Vec::new(),
            databases_by_id: BTreeMap::new(),
            databases_by_name: BTreeMap::new(),
            ambient_schemas_by_name: BTreeMap::new(),
            schemas_by_key: BTreeMap::new(),
            schemas_by_id: BTreeMap::new(),
            items_by_id: BTreeMap::new(),
            items_by_global_id: BTreeMap::new(),
            items_by_name: HashMap::new(),
            cluster,
            config,
            system_vars: SystemVars::new(),
            ids: IdAllocator::new(),
            mz_internal_schema_id: SchemaId::System(0),
            mz_unsafe_schema_id: SchemaId::System(0),
        };
        catalog.seed_system_schemas();
        catalog.seed_builtins()?;
        catalog.refresh_search_path();
        Ok(catalog)
    }

    /// Initialize a fresh catalog for one typecheck run.
    pub(super) fn open() -> Result<Self, TypeCheckError> {
        let start = Instant::now();
        let catalog = Self::new().map_err(|e| TypeCheckError::DatabaseSetupError(e.to_string()))?;
        timing!("    catalog: new", start.elapsed());
        Ok(catalog)
    }

    /// Ensure all database/schema namespaces referenced by the project and
    /// external types exist in the catalog before validation begins.
    pub(super) fn bootstrap_namespaces(
        &mut self,
        project: &super::Project,
        external_types: &super::Types,
    ) {
        let start = Instant::now();
        let mut namespaces = BTreeSet::new();
        for object in project.iter_objects() {
            namespaces.insert((object.id.database.clone(), object.id.schema.clone()));
        }
        for id in external_types.tables.keys() {
            namespaces.insert((id.database.clone(), id.schema.clone()));
        }

        for (database, schema) in namespaces {
            let namespace_start = Instant::now();
            self.ensure_user_schema(&database, &schema);
            timing!(
                &format!("      catalog: ensure_namespace {}.{}", database, schema),
                namespace_start.elapsed()
            );
        }
        timing!("    catalog: bootstrap_namespaces", start.elapsed());
    }

    /// Create a placeholder table with the given column schema, representing
    /// a cached or external dependency during validation.
    pub(super) fn create_stub_table(
        &mut self,
        object_id: &ObjectId,
        columns: &BTreeMap<String, ColumnType>,
    ) -> Result<(), TypeCheckError> {
        let build_sql_start = Instant::now();
        let sql = super::convert::create_stub_table_sql(object_id, columns);
        timing!(
            &format!("        catalog: build_stub_sql {}", object_id),
            build_sql_start.elapsed()
        );
        let create_start = Instant::now();
        let result = self
            .create_item(object_id, &sql)
            .map(|_| ())
            .map_err(TypeCheckError::TypeCheckFailed);
        timing!(
            &format!("        catalog: create_stub_table {}", object_id),
            create_start.elapsed()
        );
        result
    }

    /// Parse, resolve, and type-check a SQL statement against the catalog.
    /// On success, inserts the resulting item and returns its column schema.
    pub(super) fn create_item(
        &mut self,
        object_id: &ObjectId,
        sql: &str,
    ) -> Result<RelationDesc, ObjectTypeCheckError> {
        let parsed = mz_sql_parser::parser::parse_statements(sql)
            .map_err(|e| self.build_error(object_id, sql, ObjectTypeCheckErrorKind::Parser(e)))?
            .into_iter()
            .next()
            .ok_or_else(|| {
                self.build_error(
                    object_id,
                    sql,
                    ObjectTypeCheckErrorKind::Internal("empty statement".into()),
                )
            })?
            .ast;
        self.resolve_plan_and_insert(object_id, parsed, sql)
    }

    /// Resolve and type-check an already-parsed SQL statement, skipping parse.
    pub(super) fn create_item_from_ast(
        &mut self,
        object_id: &ObjectId,
        ast: mz_sql_parser::ast::Statement<mz_sql_parser::ast::Raw>,
    ) -> Result<RelationDesc, ObjectTypeCheckError> {
        let create_sql = ast.to_string();
        self.resolve_plan_and_insert(object_id, ast, &create_sql)
    }

    fn resolve_plan_and_insert(
        &mut self,
        object_id: &ObjectId,
        ast: mz_sql_parser::ast::Statement<mz_sql_parser::ast::Raw>,
        create_sql: &str,
    ) -> Result<RelationDesc, ObjectTypeCheckError> {
        let start = Instant::now();

        let resolve_start = Instant::now();
        let (resolved, resolved_ids) = mz_sql::names::resolve(&*self, ast).map_err(|e| {
            self.build_error(
                object_id,
                create_sql,
                ObjectTypeCheckErrorKind::Plan(Arc::new(e)),
            )
        })?;
        timing!(
            &format!("      catalog: resolve {}", object_id),
            resolve_start.elapsed()
        );

        let plan_start = Instant::now();
        let pcx = PlanContext::new(Utc::now());
        let plan = mz_sql::plan::plan(
            Some(&pcx),
            &*self,
            resolved,
            &Params::empty(),
            &resolved_ids,
        )
        .map_err(|e| {
            self.build_error(
                object_id,
                create_sql,
                ObjectTypeCheckErrorKind::Plan(Arc::new(e)),
            )
        })?;
        timing!(
            &format!("      catalog: plan {}", object_id),
            plan_start.elapsed()
        );

        let insert_start = Instant::now();
        let desc = self
            .insert_item_from_plan(object_id, create_sql, plan, resolved_ids)
            .map_err(|e| {
                self.build_error(object_id, create_sql, ObjectTypeCheckErrorKind::Catalog(e))
            })?;
        timing!(
            &format!("      catalog: insert_item {}", object_id),
            insert_start.elapsed()
        );
        timing!(
            &format!("    catalog: create_item {}", object_id),
            start.elapsed()
        );
        Ok(desc)
    }

    /// Construct a typecheck error with the object's inferred file path.
    /// The placeholder file path is rewritten to the real on-disk path by
    /// the caller in `typecheck::run`.
    fn build_error(
        &self,
        object_id: &ObjectId,
        sql: &str,
        kind: ObjectTypeCheckErrorKind,
    ) -> ObjectTypeCheckError {
        ObjectTypeCheckError {
            object_id: object_id.clone(),
            file_path: PathBuf::from(format!(
                "{}/{}/{}.sql",
                object_id.database, object_id.schema, object_id.object
            )),
            sql_statement: sql.to_string(),
            kind,
        }
    }

    /// Register all system schemas discovered from the builtin catalog.
    fn seed_system_schemas(&mut self) {
        let mut schemas = BTreeSet::new();
        for builtin in BUILTINS::iter(&self.config.builtins_cfg) {
            schemas.insert(builtin.schema().to_string());
        }
        for schema in schemas {
            let schema_id = SchemaId::System(self.ids.allocate_schema());
            if schema == "mz_internal" {
                self.mz_internal_schema_id = schema_id;
            }
            if schema == "mz_unsafe" {
                self.mz_unsafe_schema_id = schema_id;
            }
            let schema_spec = SchemaSpecifier::Id(schema_id);
            let qualified = QualifiedSchemaName {
                database: ResolvedDatabaseSpecifier::Ambient,
                schema: schema.clone(),
            };
            let local = LocalSchema {
                database: ResolvedDatabaseSpecifier::Ambient,
                name: qualified,
                id: schema_spec.clone(),
                item_ids: BTreeSet::new(),
                owner_id: MZ_SYSTEM_ROLE_ID,
                privileges: PrivilegeMap::default(),
            };
            self.ambient_schemas_by_name
                .insert(schema.clone(), schema_spec.clone());
            self.schemas_by_key.insert(
                (ResolvedDatabaseSpecifier::Ambient, schema.clone()),
                local.clone(),
            );
            self.schemas_by_id
                .insert((ResolvedDatabaseSpecifier::Ambient, schema_spec), local);
        }
    }

    /// Register all builtin items (types, functions, tables, views) from
    /// Materialize's built-in catalog.
    fn seed_builtins(&mut self) -> Result<(), CatalogError> {
        let builtins: Vec<_> = BUILTINS::iter(&self.config.builtins_cfg).collect();
        for builtin in builtins {
            self.insert_builtin(builtin)?;
        }
        Ok(())
    }

    /// Register a single builtin item, allocating IDs and resolving type
    /// references.
    fn insert_builtin(&mut self, builtin: &Builtin<NameReference>) -> Result<(), CatalogError> {
        let schema_spec = self
            .ambient_schemas_by_name
            .get(builtin.schema())
            .cloned()
            .ok_or_else(|| CatalogError::UnknownSchema(builtin.schema().into()))?;
        let name = QualifiedItemName {
            qualifiers: ItemQualifiers {
                database_spec: ResolvedDatabaseSpecifier::Ambient,
                schema_spec: schema_spec.clone(),
            },
            item: builtin.name().into(),
        };
        let (item_type, create_sql, relation_desc, func, type_details) = match builtin {
            Builtin::Log(log) => (
                CatalogItemType::Source,
                format!("CREATE SOURCE {}.{}", log.schema, log.name),
                None,
                None,
                None,
            ),
            Builtin::Table(table) => (
                CatalogItemType::Table,
                format!("CREATE TABLE {}.{}", table.schema, table.name),
                Some(table.desc.clone()),
                None,
                None,
            ),
            Builtin::View(view) => (
                CatalogItemType::View,
                view.create_sql(),
                Some(view.desc.clone()),
                None,
                None,
            ),
            Builtin::MaterializedView(mv) => (
                CatalogItemType::MaterializedView,
                format!(
                    "CREATE MATERIALIZED VIEW {}.{} {}",
                    mv.schema, mv.name, mv.sql
                ),
                Some(mv.desc.clone()),
                None,
                None,
            ),
            Builtin::Type(typ) => (
                CatalogItemType::Type,
                format!("CREATE TYPE {}.{}", typ.schema, typ.name),
                None,
                None,
                Some(self.resolve_builtin_type_references(typ).details),
            ),
            Builtin::Func(func) => (
                CatalogItemType::Func,
                format!("FUNCTION {}.{}", func.schema, func.name),
                None,
                Some(func.inner),
                None,
            ),
            Builtin::Source(source) => (
                CatalogItemType::Source,
                format!("CREATE SOURCE {}.{}", source.schema, source.name),
                Some(source.desc.clone()),
                None,
                None,
            ),
            Builtin::ContinualTask(ct) => (
                CatalogItemType::ContinualTask,
                ct.sql.into(),
                Some(ct.desc.clone()),
                None,
                None,
            ),
            Builtin::Index(index) => (CatalogItemType::Index, index.sql.into(), None, None, None),
            Builtin::Connection(connection) => (
                CatalogItemType::Connection,
                format!(
                    "CREATE CONNECTION {}.{}",
                    connection.schema, connection.name
                ),
                None,
                None,
                None,
            ),
        };

        let item_id = CatalogItemId::System(self.ids.allocate_item());
        let global_id = GlobalId::System(self.ids.allocate_global());
        let oid = match builtin {
            Builtin::Log(log) => log.oid,
            Builtin::Table(table) => table.oid,
            Builtin::View(view) => view.oid,
            Builtin::MaterializedView(mv) => mv.oid,
            Builtin::Type(typ) => typ.oid,
            Builtin::Func(_) => self.ids.allocate_oid()?,
            Builtin::Source(source) => source.oid,
            Builtin::ContinualTask(ct) => ct.oid,
            Builtin::Index(index) => index.oid,
            Builtin::Connection(connection) => connection.oid,
        };

        let item = LocalItem {
            name: name.clone(),
            id: item_id,
            global_id,
            oid,
            item_type,
            create_sql,
            references: ResolvedIds::empty(),
            uses: BTreeSet::new(),
            referenced_by: Vec::new(),
            used_by: Vec::new(),
            relation_desc,
            func,
            type_details,
            owner_id: MZ_SYSTEM_ROLE_ID,
            privileges: PrivilegeMap::default(),
            cluster_id: None,
        };
        self.insert_item(item);
        Ok(())
    }

    /// Resolve name-based type references in builtin type definitions to
    /// ID-based references, enabling the catalog to track type relationships.
    fn resolve_builtin_type_references(
        &self,
        builtin: &BuiltinType<NameReference>,
    ) -> BuiltinType<IdReference> {
        let typ: CatalogType<IdReference> = match &builtin.details.typ {
            CatalogType::AclItem => CatalogType::AclItem,
            CatalogType::Array { element_reference } => CatalogType::Array {
                element_reference: self.get_system_type(element_reference).id(),
            },
            CatalogType::List {
                element_reference,
                element_modifiers,
            } => CatalogType::List {
                element_reference: self.get_system_type(element_reference).id(),
                element_modifiers: element_modifiers.clone(),
            },
            CatalogType::Map {
                key_reference,
                value_reference,
                key_modifiers,
                value_modifiers,
            } => CatalogType::Map {
                key_reference: self.get_system_type(key_reference).id(),
                value_reference: self.get_system_type(value_reference).id(),
                key_modifiers: key_modifiers.clone(),
                value_modifiers: value_modifiers.clone(),
            },
            CatalogType::Range { element_reference } => CatalogType::Range {
                element_reference: self.get_system_type(element_reference).id(),
            },
            CatalogType::Record { fields } => CatalogType::Record {
                fields: fields
                    .iter()
                    .map(|f| mz_sql::catalog::CatalogRecordField {
                        name: f.name.clone(),
                        type_reference: self.get_system_type(f.type_reference).id(),
                        type_modifiers: f.type_modifiers.clone(),
                    })
                    .collect(),
            },
            CatalogType::Bool => CatalogType::Bool,
            CatalogType::Bytes => CatalogType::Bytes,
            CatalogType::Char => CatalogType::Char,
            CatalogType::Date => CatalogType::Date,
            CatalogType::Float32 => CatalogType::Float32,
            CatalogType::Float64 => CatalogType::Float64,
            CatalogType::Int16 => CatalogType::Int16,
            CatalogType::Int32 => CatalogType::Int32,
            CatalogType::Int64 => CatalogType::Int64,
            CatalogType::UInt16 => CatalogType::UInt16,
            CatalogType::UInt32 => CatalogType::UInt32,
            CatalogType::UInt64 => CatalogType::UInt64,
            CatalogType::MzTimestamp => CatalogType::MzTimestamp,
            CatalogType::Interval => CatalogType::Interval,
            CatalogType::Jsonb => CatalogType::Jsonb,
            CatalogType::Numeric => CatalogType::Numeric,
            CatalogType::Oid => CatalogType::Oid,
            CatalogType::PgLegacyChar => CatalogType::PgLegacyChar,
            CatalogType::PgLegacyName => CatalogType::PgLegacyName,
            CatalogType::Pseudo => CatalogType::Pseudo,
            CatalogType::RegClass => CatalogType::RegClass,
            CatalogType::RegProc => CatalogType::RegProc,
            CatalogType::RegType => CatalogType::RegType,
            CatalogType::String => CatalogType::String,
            CatalogType::Time => CatalogType::Time,
            CatalogType::Timestamp => CatalogType::Timestamp,
            CatalogType::TimestampTz => CatalogType::TimestampTz,
            CatalogType::Uuid => CatalogType::Uuid,
            CatalogType::VarChar => CatalogType::VarChar,
            CatalogType::Int2Vector => CatalogType::Int2Vector,
            CatalogType::MzAclItem => CatalogType::MzAclItem,
        };

        BuiltinType {
            name: builtin.name,
            schema: builtin.schema,
            oid: builtin.oid,
            details: CatalogTypeDetails {
                array_id: builtin.details.array_id,
                typ,
                pg_metadata: builtin.details.pg_metadata.clone(),
            },
        }
    }

    /// Add a fully-constructed item to the catalog, updating all lookup
    /// indexes (by name, by ID, by schema).
    fn insert_item(&mut self, item: LocalItem) {
        let schema_key = (
            item.name.qualifiers.database_spec,
            self.resolve_full_schema_name(&QualifiedSchemaName {
                database: item.name.qualifiers.database_spec,
                schema: self.schema_name(
                    &item.name.qualifiers.database_spec,
                    &item.name.qualifiers.schema_spec,
                ),
            })
            .schema,
        );
        if let Some(schema) = self.schemas_by_key.get_mut(&schema_key) {
            schema.item_ids.insert(item.id);
        }
        if let Some(schema) = self.schemas_by_id.get_mut(&(
            item.name.qualifiers.database_spec,
            item.name.qualifiers.schema_spec.clone(),
        )) {
            schema.item_ids.insert(item.id);
        }
        self.items_by_global_id.insert(item.global_id, item.id);
        self.items_by_name
            .entry(item.name.clone())
            .or_default()
            .push(item.id);
        self.items_by_id.insert(item.id, Arc::new(item));
    }

    /// Ensure the given database and schema exist, creating them if needed.
    /// Updates the active database and search path when new databases are added.
    fn ensure_user_schema(&mut self, database_name: &str, schema_name: &str) {
        let database_id = match self.databases_by_name.get(database_name).copied() {
            Some(id) => id,
            None => {
                let id = mz_sql::names::DatabaseId::User(self.ids.allocate_database());
                self.databases_by_name.insert(database_name.into(), id);
                self.databases_by_id.insert(
                    id,
                    LocalDatabase {
                        name: database_name.into(),
                        id,
                        schema_ids: BTreeMap::new(),
                        owner_id: MZ_SYSTEM_ROLE_ID,
                        privileges: PrivilegeMap::default(),
                    },
                );
                if self.active_database.is_none() {
                    self.active_database = Some(id);
                }
                id
            }
        };
        let database_spec = ResolvedDatabaseSpecifier::Id(database_id);
        if self
            .schemas_by_key
            .contains_key(&(database_spec, schema_name.to_string()))
        {
            self.refresh_search_path();
            return;
        }
        let schema_id = SchemaId::User(self.ids.allocate_schema());
        let schema_spec = SchemaSpecifier::Id(schema_id);
        let qualified = QualifiedSchemaName {
            database: database_spec,
            schema: schema_name.into(),
        };
        let local = LocalSchema {
            database: database_spec,
            name: qualified,
            id: schema_spec.clone(),
            item_ids: BTreeSet::new(),
            owner_id: MZ_SYSTEM_ROLE_ID,
            privileges: PrivilegeMap::default(),
        };
        self.databases_by_id
            .get_mut(&database_id)
            .expect("database exists")
            .schema_ids
            .insert(schema_name.into(), schema_id);
        self.schemas_by_key
            .insert((database_spec, schema_name.into()), local.clone());
        self.schemas_by_id
            .insert((database_spec, schema_spec), local);
        self.refresh_search_path();
    }

    /// Rebuild the search path: pg_catalog, then the active database's public
    /// schema, then mz_catalog, mz_internal, mz_unsafe, information_schema.
    fn refresh_search_path(&mut self) {
        let mut search_path = Vec::new();
        if let Some(schema) = self.ambient_schemas_by_name.get("pg_catalog") {
            search_path.push((ResolvedDatabaseSpecifier::Ambient, schema.clone()));
        }
        if let Some(database_id) = self.active_database {
            let database_spec = ResolvedDatabaseSpecifier::Id(database_id);
            if let Some(database) = self.databases_by_id.get(&database_id) {
                if let Some(public_id) = database.schema_ids.get("public") {
                    search_path.push((database_spec, SchemaSpecifier::Id(*public_id)));
                }
            }
        }
        for schema_name in [
            "mz_catalog",
            "mz_internal",
            "mz_unsafe",
            "information_schema",
        ] {
            if let Some(schema) = self.ambient_schemas_by_name.get(schema_name) {
                search_path.push((ResolvedDatabaseSpecifier::Ambient, schema.clone()));
            }
        }
        self.search_path = search_path;
    }

    /// Insert a SQL-planned item (table, view, or materialized view) into the
    /// catalog, extracting its column schema and dependency references. Returns
    /// the item's output column description.
    fn insert_item_from_plan(
        &mut self,
        object_id: &ObjectId,
        sql: &str,
        plan: Plan,
        resolved_ids: ResolvedIds,
    ) -> Result<RelationDesc, CatalogError> {
        let item_id = CatalogItemId::User(self.ids.allocate_item());
        let global_id = GlobalId::User(self.ids.allocate_global());
        let oid = self.ids.allocate_oid()?;

        let (name, item_type, desc, uses, cluster_id) = match plan {
            Plan::CreateTable(plan) => (
                plan.name,
                CatalogItemType::Table,
                plan.table.desc.latest(),
                BTreeSet::new(),
                None,
            ),
            Plan::CreateView(plan) => {
                let desc = RelationDesc::new(
                    plan.view.expr.top_level_typ(),
                    plan.view.column_names.clone(),
                );
                (
                    plan.name,
                    CatalogItemType::View,
                    desc,
                    plan.view.dependencies.0,
                    None,
                )
            }
            Plan::CreateMaterializedView(plan) => {
                let desc = RelationDesc::new(
                    plan.materialized_view.expr.top_level_typ(),
                    plan.materialized_view.column_names.clone(),
                );
                (
                    plan.name,
                    CatalogItemType::MaterializedView,
                    desc,
                    plan.materialized_view.dependencies.0,
                    Some(plan.materialized_view.cluster_id),
                )
            }
            _ => {
                return Err(CatalogError::UnexpectedType {
                    name: object_id.to_string(),
                    actual_type: CatalogItemType::Source,
                    expected_type: CatalogItemType::Table,
                });
            }
        };

        let item = LocalItem {
            name,
            id: item_id,
            global_id,
            oid,
            item_type,
            create_sql: sql.into(),
            references: resolved_ids,
            uses,
            referenced_by: Vec::new(),
            used_by: Vec::new(),
            relation_desc: Some(desc.clone()),
            func: None,
            type_details: None,
            owner_id: MZ_SYSTEM_ROLE_ID,
            privileges: PrivilegeMap::default(),
            cluster_id,
        };
        self.insert_item(item);
        Ok(desc)
    }

    /// Insert a stub Table item directly from a [`RelationDesc`], skipping
    /// parse, name resolution, and SQL planning entirely.
    ///
    /// Used by the parallel typechecker to materialize an upstream view's
    /// schema in a downstream task's catalog without re-running the
    /// resolve+plan pipeline for every (consumer, dep) pair.
    pub(super) fn insert_stub_table_with_desc(
        &mut self,
        object_id: &ObjectId,
        desc: RelationDesc,
    ) -> Result<(), TypeCheckError> {
        self.insert_stub_table_with_desc_inner(object_id, desc)
            .map_err(|e| TypeCheckError::DatabaseSetupError(e.to_string()))
    }

    fn insert_stub_table_with_desc_inner(
        &mut self,
        object_id: &ObjectId,
        desc: RelationDesc,
    ) -> Result<(), CatalogError> {
        let database_id = *self
            .databases_by_name
            .get(&object_id.database)
            .ok_or_else(|| CatalogError::UnknownDatabase(object_id.database.clone()))?;
        let database_spec = ResolvedDatabaseSpecifier::Id(database_id);
        let schema = self
            .schemas_by_key
            .get(&(database_spec, object_id.schema.clone()))
            .ok_or_else(|| CatalogError::UnknownSchema(object_id.schema.clone()))?;
        let schema_spec = schema.id.clone();

        let item_id = CatalogItemId::User(self.ids.allocate_item());
        let global_id = GlobalId::User(self.ids.allocate_global());
        let oid = self.ids.allocate_oid()?;

        let item = LocalItem {
            name: QualifiedItemName {
                qualifiers: ItemQualifiers {
                    database_spec,
                    schema_spec,
                },
                item: object_id.object.clone(),
            },
            id: item_id,
            global_id,
            oid,
            item_type: CatalogItemType::Table,
            create_sql: String::new(),
            references: ResolvedIds::empty(),
            uses: BTreeSet::new(),
            referenced_by: Vec::new(),
            used_by: Vec::new(),
            relation_desc: Some(desc),
            func: None,
            type_details: None,
            owner_id: MZ_SYSTEM_ROLE_ID,
            privileges: PrivilegeMap::default(),
            cluster_id: None,
        };
        self.insert_item(item);
        Ok(())
    }

    fn schema_name(
        &self,
        database_spec: &ResolvedDatabaseSpecifier,
        schema_spec: &SchemaSpecifier,
    ) -> String {
        self.schemas_by_id
            .get(&(*database_spec, schema_spec.clone()))
            .expect("schema exists")
            .name
            .schema
            .clone()
    }

    /// Resolve a partially-qualified name against the search path, applying a
    /// predicate filter to distinguish between items, functions, and types that
    /// may share the same qualified name.
    fn lookup_item_by_partial_name(
        &self,
        name: &PartialItemName,
        predicate: impl Fn(&LocalItem) -> bool,
        unknown: impl Fn(String) -> CatalogError,
    ) -> Result<&LocalItem, CatalogError> {
        if let Some(schema_name) = name.schema.as_deref() {
            let database_spec = if self.ambient_schemas_by_name.contains_key(schema_name) {
                ResolvedDatabaseSpecifier::Ambient
            } else {
                let db = match name.database.as_deref() {
                    Some(database) => *self
                        .databases_by_name
                        .get(database)
                        .ok_or_else(|| CatalogError::UnknownDatabase(database.into()))?,
                    None => self
                        .active_database
                        .ok_or_else(|| CatalogError::UnknownSchema(schema_name.into()))?,
                };
                ResolvedDatabaseSpecifier::Id(db)
            };
            let schema_spec = self
                .resolve_schema_in_database(&database_spec, schema_name)?
                .id()
                .clone();
            let qualified = QualifiedItemName {
                qualifiers: ItemQualifiers {
                    database_spec,
                    schema_spec,
                },
                item: name.item.clone(),
            };
            let item_ids = self
                .items_by_name
                .get(&qualified)
                .ok_or_else(|| unknown(name.to_string()))?;
            for item_id in item_ids {
                let item = self.items_by_id.get(item_id).expect("item exists");
                if predicate(item) {
                    return Ok(item.as_ref());
                }
            }
            Err(unknown(name.to_string()))
        } else {
            for (database_spec, schema_spec) in &self.search_path {
                let qualified = QualifiedItemName {
                    qualifiers: ItemQualifiers {
                        database_spec: *database_spec,
                        schema_spec: schema_spec.clone(),
                    },
                    item: name.item.clone(),
                };
                if let Some(item_ids) = self.items_by_name.get(&qualified) {
                    for item_id in item_ids {
                        let item = self.items_by_id.get(item_id).expect("item exists");
                        if predicate(item) {
                            return Ok(item.as_ref());
                        }
                    }
                }
            }
            Err(unknown(name.to_string()))
        }
    }
}

/// Provides human-readable names for expression display. Maps `GlobalId`s
/// to qualified item names and column names.
impl ExprHumanizer for CatalogRuntime {
    fn humanize_id(&self, id: GlobalId) -> Option<String> {
        self.items_by_global_id
            .get(&id)
            .and_then(|item_id| self.items_by_id.get(item_id))
            .map(|item| self.resolve_full_name(item.name()).to_string())
    }

    fn humanize_id_unqualified(&self, id: GlobalId) -> Option<String> {
        self.items_by_global_id
            .get(&id)
            .and_then(|item_id| self.items_by_id.get(item_id))
            .map(|item| item.name.item.clone())
    }

    fn humanize_id_parts(&self, id: GlobalId) -> Option<Vec<String>> {
        self.items_by_global_id
            .get(&id)
            .and_then(|item_id| self.items_by_id.get(item_id))
            .map(|item| {
                let full = self.resolve_full_name(item.name());
                let mut parts = Vec::new();
                if let RawDatabaseSpecifier::Name(database) = full.database {
                    parts.push(database);
                }
                parts.push(full.schema);
                parts.push(full.item);
                parts
            })
    }

    fn humanize_sql_scalar_type(&self, ty: &SqlScalarType, postgres_compat: bool) -> String {
        DummyHumanizer.humanize_sql_scalar_type(ty, postgres_compat)
    }

    fn column_names_for_id(&self, id: GlobalId) -> Option<Vec<String>> {
        self.items_by_global_id
            .get(&id)
            .and_then(|item_id| self.items_by_id.get(item_id))
            .and_then(|item| item.relation_desc.as_ref())
            .map(|desc| {
                desc.iter_names()
                    .map(|name| name.as_str().to_string())
                    .collect()
            })
    }

    fn humanize_column(&self, id: GlobalId, column: usize) -> Option<String> {
        self.items_by_global_id
            .get(&id)
            .and_then(|item_id| self.items_by_id.get(item_id))
            .and_then(|item| item.relation_desc.as_ref())
            .map(|desc| desc.get_name(column).to_string())
    }

    fn id_exists(&self, id: GlobalId) -> bool {
        self.items_by_global_id.contains_key(&id)
    }
}

/// Inline connection resolution isn't part of project SQL — never reached during typecheck.
impl ConnectionResolver for CatalogRuntime {
    fn resolve_connection(&self, id: CatalogItemId) -> Connection<InlinedConnection> {
        unreachable!("catalog backend cannot resolve connection {id}")
    }
}

/// Core trait providing name resolution, item lookup, and session state for
/// the SQL planner. Most methods delegate to the catalog's internal indexes;
/// unsupported operations (replicas, network policies, connections) return
/// errors or empty results.
#[allow(clippy::as_conversions)] // Trait object coercions are unavoidable in this impl
impl SessionCatalog for CatalogRuntime {
    fn active_role_id(&self) -> &RoleId {
        &self.active_role.id
    }

    fn active_database(&self) -> Option<&mz_sql::names::DatabaseId> {
        self.active_database.as_ref()
    }

    fn active_cluster(&self) -> &str {
        &self.active_cluster_name
    }

    fn search_path(&self) -> &[(ResolvedDatabaseSpecifier, SchemaSpecifier)] {
        &self.search_path
    }

    fn get_prepared_statement_desc(&self, _name: &str) -> Option<&StatementDesc> {
        None
    }

    fn get_portal_desc_unverified(&self, _portal_name: &str) -> Option<&StatementDesc> {
        None
    }

    fn resolve_database(&self, database_name: &str) -> Result<&dyn CatalogDatabase, CatalogError> {
        let id = self
            .databases_by_name
            .get(database_name)
            .ok_or_else(|| CatalogError::UnknownDatabase(database_name.into()))?;
        Ok(self.databases_by_id.get(id).expect("database exists"))
    }

    fn get_database(&self, id: &mz_sql::names::DatabaseId) -> &dyn CatalogDatabase {
        self.databases_by_id.get(id).expect("database exists")
    }

    fn get_databases(&self) -> Vec<&dyn CatalogDatabase> {
        self.databases_by_id
            .values()
            .map(|database| database as &dyn CatalogDatabase)
            .collect()
    }

    fn resolve_schema(
        &self,
        database_name: Option<&str>,
        schema_name: &str,
    ) -> Result<&dyn CatalogSchema, CatalogError> {
        let database_spec = if self.ambient_schemas_by_name.contains_key(schema_name) {
            ResolvedDatabaseSpecifier::Ambient
        } else if let Some(database_name) = database_name {
            let database_id = *self
                .databases_by_name
                .get(database_name)
                .ok_or_else(|| CatalogError::UnknownDatabase(database_name.into()))?;
            ResolvedDatabaseSpecifier::Id(database_id)
        } else {
            ResolvedDatabaseSpecifier::Id(
                self.active_database
                    .ok_or_else(|| CatalogError::UnknownSchema(schema_name.into()))?,
            )
        };
        self.resolve_schema_in_database(&database_spec, schema_name)
    }

    fn resolve_schema_in_database(
        &self,
        database_spec: &ResolvedDatabaseSpecifier,
        schema_name: &str,
    ) -> Result<&dyn CatalogSchema, CatalogError> {
        self.schemas_by_key
            .get(&(*database_spec, schema_name.into()))
            .map(|schema| schema as &dyn CatalogSchema)
            .ok_or_else(|| CatalogError::UnknownSchema(schema_name.into()))
    }

    fn get_schema(
        &self,
        database_spec: &ResolvedDatabaseSpecifier,
        schema_spec: &SchemaSpecifier,
    ) -> &dyn CatalogSchema {
        self.schemas_by_id
            .get(&(*database_spec, schema_spec.clone()))
            .expect("schema exists")
    }

    fn get_schemas(&self) -> Vec<&dyn CatalogSchema> {
        self.schemas_by_id
            .values()
            .map(|schema| schema as &dyn CatalogSchema)
            .collect()
    }

    fn get_mz_internal_schema_id(&self) -> SchemaId {
        self.mz_internal_schema_id
    }

    fn get_mz_unsafe_schema_id(&self) -> SchemaId {
        self.mz_unsafe_schema_id
    }

    fn is_system_schema_specifier(&self, schema: SchemaSpecifier) -> bool {
        matches!(schema, SchemaSpecifier::Id(SchemaId::System(_)))
    }

    fn resolve_role(&self, role_name: &str) -> Result<&dyn CatalogRole, CatalogError> {
        if role_name == self.active_role.name {
            Ok(&self.active_role)
        } else {
            Err(CatalogError::UnknownRole(role_name.into()))
        }
    }

    fn resolve_network_policy(
        &self,
        network_policy_name: &str,
    ) -> Result<&dyn CatalogNetworkPolicy, CatalogError> {
        Err(CatalogError::UnknownNetworkPolicy(
            network_policy_name.into(),
        ))
    }

    fn try_get_role(&self, id: &RoleId) -> Option<&dyn CatalogRole> {
        if *id == self.active_role.id {
            Some(&self.active_role)
        } else {
            None
        }
    }

    fn get_role(&self, id: &RoleId) -> &dyn CatalogRole {
        self.try_get_role(id).expect("role exists")
    }

    fn get_roles(&self) -> Vec<&dyn CatalogRole> {
        vec![&self.active_role]
    }

    fn mz_system_role_id(&self) -> RoleId {
        MZ_SYSTEM_ROLE_ID
    }

    fn collect_role_membership(&self, _id: &RoleId) -> BTreeSet<RoleId> {
        BTreeSet::new()
    }

    fn get_network_policy(&self, _id: &NetworkPolicyId) -> &dyn CatalogNetworkPolicy {
        // Network policies aren't part of project SQL — never reached during typecheck.
        unreachable!("catalog backend has no network policies")
    }

    fn get_network_policies(&self) -> Vec<&dyn CatalogNetworkPolicy> {
        Vec::new()
    }

    fn resolve_cluster<'a, 'b>(
        &'a self,
        cluster_name: Option<&'b str>,
    ) -> Result<&'a dyn CatalogCluster<'a>, CatalogError> {
        match cluster_name {
            None => Ok(&self.cluster),
            Some(name) if name == self.cluster.name => Ok(&self.cluster),
            Some(name) => Err(CatalogError::UnknownCluster(name.into())),
        }
    }

    fn resolve_cluster_replica<'a, 'b>(
        &'a self,
        cluster_replica_name: &'b mz_sql_parser::ast::QualifiedReplica,
    ) -> Result<&'a dyn CatalogClusterReplica<'a>, CatalogError> {
        Err(CatalogError::UnknownClusterReplica(
            cluster_replica_name.to_string(),
        ))
    }

    fn resolve_item(&self, item_name: &PartialItemName) -> Result<&dyn CatalogItem, CatalogError> {
        self.lookup_item_by_partial_name(
            item_name,
            |item| {
                item.item_type != CatalogItemType::Func && item.item_type != CatalogItemType::Type
            },
            CatalogError::UnknownItem,
        )
        .map(|item| item as &dyn CatalogItem)
    }

    fn resolve_function(
        &self,
        item_name: &PartialItemName,
    ) -> Result<&dyn CatalogItem, CatalogError> {
        self.lookup_item_by_partial_name(
            item_name,
            |item| item.item_type == CatalogItemType::Func,
            |name| CatalogError::UnknownFunction {
                name,
                alternative: None,
            },
        )
        .map(|item| item as &dyn CatalogItem)
    }

    fn resolve_type(&self, item_name: &PartialItemName) -> Result<&dyn CatalogItem, CatalogError> {
        self.lookup_item_by_partial_name(
            item_name,
            |item| item.item_type == CatalogItemType::Type,
            |name| CatalogError::UnknownType { name },
        )
        .map(|item| item as &dyn CatalogItem)
    }

    fn get_system_type(&self, name: &str) -> &dyn CatalogItem {
        for schema_name in ["pg_catalog", "mz_catalog"] {
            if let Some(schema_spec) = self.ambient_schemas_by_name.get(schema_name) {
                let qualified = QualifiedItemName {
                    qualifiers: ItemQualifiers {
                        database_spec: ResolvedDatabaseSpecifier::Ambient,
                        schema_spec: schema_spec.clone(),
                    },
                    item: name.into(),
                };
                if let Some(item_ids) = self.items_by_name.get(&qualified) {
                    for item_id in item_ids {
                        let item = self.items_by_id.get(item_id).expect("system type exists");
                        if item.item_type == CatalogItemType::Type {
                            return item.as_ref();
                        }
                    }
                }
            }
        }
        panic!("system type does not exist: {name}")
    }

    fn try_get_item(&self, id: &CatalogItemId) -> Option<&dyn CatalogItem> {
        self.items_by_id
            .get(id)
            .map(|item| item.as_ref() as &dyn CatalogItem)
    }

    fn try_get_item_by_global_id<'a>(
        &'a self,
        id: &GlobalId,
    ) -> Option<Box<dyn mz_sql::catalog::CatalogCollectionItem + 'a>> {
        self.items_by_global_id
            .get(id)
            .and_then(|item_id| self.items_by_id.get(item_id))
            .map(|item| {
                Box::new(LocalItem::clone(item)) as Box<dyn mz_sql::catalog::CatalogCollectionItem>
            })
    }

    fn get_item(&self, id: &CatalogItemId) -> &dyn CatalogItem {
        self.items_by_id.get(id).expect("item exists").as_ref()
    }

    fn get_item_by_global_id<'a>(
        &'a self,
        id: &GlobalId,
    ) -> Box<dyn mz_sql::catalog::CatalogCollectionItem + 'a> {
        self.try_get_item_by_global_id(id).expect("item exists")
    }

    fn get_items(&self) -> Vec<&dyn CatalogItem> {
        self.items_by_id
            .values()
            .map(|item| item.as_ref() as &dyn CatalogItem)
            .collect()
    }

    fn get_item_by_name(&self, name: &QualifiedItemName) -> Option<&dyn CatalogItem> {
        self.items_by_name
            .get(name)
            .and_then(|item_ids| item_ids.first())
            .and_then(|item_id| self.items_by_id.get(item_id))
            .map(|item| item.as_ref() as &dyn CatalogItem)
    }

    fn get_type_by_name(&self, name: &QualifiedItemName) -> Option<&dyn CatalogItem> {
        self.items_by_name.get(name).and_then(|item_ids| {
            item_ids.iter().find_map(|item_id| {
                let item = self.items_by_id.get(item_id)?;
                if item.item_type == CatalogItemType::Type {
                    Some(item.as_ref() as &dyn CatalogItem)
                } else {
                    None
                }
            })
        })
    }

    fn get_cluster(&self, _id: ClusterId) -> &dyn CatalogCluster<'_> {
        &self.cluster
    }

    fn get_clusters(&self) -> Vec<&dyn CatalogCluster<'_>> {
        vec![&self.cluster]
    }

    fn get_cluster_replica(
        &self,
        _cluster_id: ClusterId,
        _replica_id: ReplicaId,
    ) -> &dyn CatalogClusterReplica<'_> {
        // Cluster replicas aren't part of project SQL — never reached during typecheck.
        unreachable!("catalog backend has no replicas")
    }

    fn get_cluster_replicas(&self) -> Vec<&dyn CatalogClusterReplica<'_>> {
        Vec::new()
    }

    fn get_system_privileges(&self) -> &PrivilegeMap {
        static EMPTY: std::sync::LazyLock<PrivilegeMap> =
            std::sync::LazyLock::new(PrivilegeMap::default);
        &EMPTY
    }

    fn get_default_privileges(
        &self,
    ) -> Vec<(&DefaultPrivilegeObject, Vec<&DefaultPrivilegeAclItem>)> {
        Vec::new()
    }

    fn find_available_name(&self, name: QualifiedItemName) -> QualifiedItemName {
        name
    }

    fn resolve_full_name(&self, name: &QualifiedItemName) -> FullItemName {
        let schema = self
            .schemas_by_id
            .get(&(
                name.qualifiers.database_spec,
                name.qualifiers.schema_spec.clone(),
            ))
            .expect("schema exists");
        let database = match name.qualifiers.database_spec {
            ResolvedDatabaseSpecifier::Ambient => RawDatabaseSpecifier::Ambient,
            ResolvedDatabaseSpecifier::Id(id) => RawDatabaseSpecifier::Name(
                self.databases_by_id
                    .get(&id)
                    .expect("db exists")
                    .name
                    .clone(),
            ),
        };
        FullItemName {
            database,
            schema: schema.name.schema.clone(),
            item: name.item.clone(),
        }
    }

    fn resolve_full_schema_name(&self, name: &QualifiedSchemaName) -> FullSchemaName {
        let database = match name.database {
            ResolvedDatabaseSpecifier::Ambient => RawDatabaseSpecifier::Ambient,
            ResolvedDatabaseSpecifier::Id(id) => RawDatabaseSpecifier::Name(
                self.databases_by_id
                    .get(&id)
                    .expect("db exists")
                    .name
                    .clone(),
            ),
        };
        FullSchemaName {
            database,
            schema: name.schema.clone(),
        }
    }

    fn resolve_item_id(&self, global_id: &GlobalId) -> CatalogItemId {
        *self.items_by_global_id.get(global_id).expect("item exists")
    }

    fn resolve_global_id(
        &self,
        item_id: &CatalogItemId,
        _version: RelationVersionSelector,
    ) -> GlobalId {
        self.items_by_id
            .get(item_id)
            .expect("item exists")
            .global_id
    }

    fn config(&self) -> &CatalogConfig {
        &self.config
    }

    fn now(&self) -> mz_ore::now::EpochMillis {
        0
    }

    fn aws_privatelink_availability_zones(&self) -> Option<BTreeSet<String>> {
        None
    }

    fn system_vars(&self) -> &SystemVars {
        &self.system_vars
    }

    fn system_vars_mut(&mut self) -> &mut SystemVars {
        &mut self.system_vars
    }

    fn get_owner_id(&self, id: &mz_sql::names::ObjectId) -> Option<RoleId> {
        match id {
            mz_sql::names::ObjectId::Item(item_id) => {
                self.items_by_id.get(item_id).map(|item| item.owner_id)
            }
            mz_sql::names::ObjectId::Database(_) | mz_sql::names::ObjectId::Schema(_) => {
                Some(MZ_SYSTEM_ROLE_ID)
            }
            mz_sql::names::ObjectId::Cluster(_) => Some(MZ_SYSTEM_ROLE_ID),
            _ => None,
        }
    }

    fn get_privileges(&self, _id: &mz_sql::names::SystemObjectId) -> Option<&PrivilegeMap> {
        None
    }

    fn object_dependents(
        &self,
        ids: &Vec<mz_sql::names::ObjectId>,
    ) -> Vec<mz_sql::names::ObjectId> {
        ids.clone()
    }

    fn item_dependents(&self, id: CatalogItemId) -> Vec<mz_sql::names::ObjectId> {
        vec![mz_sql::names::ObjectId::Item(id)]
    }

    fn all_object_privileges(&self, _object_type: SystemObjectType) -> AclMode {
        AclMode::empty()
    }

    fn get_object_type(&self, object_id: &mz_sql::names::ObjectId) -> SqlObjectType {
        match object_id {
            mz_sql::names::ObjectId::Item(item_id) => self
                .items_by_id
                .get(item_id)
                .expect("item exists")
                .item_type
                .into(),
            mz_sql::names::ObjectId::Database(_) => SqlObjectType::Database,
            mz_sql::names::ObjectId::Schema(_) => SqlObjectType::Schema,
            mz_sql::names::ObjectId::Cluster(_) => SqlObjectType::Cluster,
            mz_sql::names::ObjectId::Role(_) => SqlObjectType::Role,
            mz_sql::names::ObjectId::ClusterReplica(_) => SqlObjectType::ClusterReplica,
            mz_sql::names::ObjectId::NetworkPolicy(_) => SqlObjectType::NetworkPolicy,
        }
    }

    fn get_system_object_type(&self, id: &mz_sql::names::SystemObjectId) -> SystemObjectType {
        match id {
            mz_sql::names::SystemObjectId::Object(object_id) => {
                SystemObjectType::Object(self.get_object_type(object_id))
            }
            mz_sql::names::SystemObjectId::System => SystemObjectType::System,
        }
    }

    fn minimal_qualification(&self, qualified_name: &QualifiedItemName) -> PartialItemName {
        let full_name = self.resolve_full_name(qualified_name);
        PartialItemName::from(full_name)
    }

    fn add_notice(&self, _notice: mz_sql::plan::PlanNotice) {}

    fn get_item_comments(&self, _id: &CatalogItemId) -> Option<&BTreeMap<Option<usize>, String>> {
        None
    }

    fn is_cluster_size_cc(&self, _size: &str) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_sql::catalog::SessionCatalog;

    #[mz_ore::test]
    fn test_resolve_builtin_types() {
        let catalog = CatalogRuntime::new().expect("catalog creation should succeed");
        let types_to_check = [
            "date",
            "time",
            "timestamp",
            "timestamptz",
            "bool",
            "text",
            "int4",
            "int8",
            "float8",
            "numeric",
            "varchar",
            "bytea",
            "jsonb",
            "uuid",
        ];
        for type_name in types_to_check {
            let partial = PartialItemName {
                database: None,
                schema: None,
                item: type_name.to_string(),
            };
            let result = catalog.resolve_type(&partial);
            assert!(
                result.is_ok(),
                "resolve_type({:?}) failed: {:?}",
                type_name,
                result.err()
            );
        }
    }

    #[mz_ore::test]
    fn test_create_table_with_date_column() {
        let mut runtime = CatalogRuntime::new().expect("catalog creation should succeed");
        runtime.ensure_user_schema("test_db", "test_schema");
        let object_id = ObjectId {
            database: "test_db".into(),
            schema: "test_schema".into(),
            object: "test_table".into(),
        };
        let sql = r#"CREATE TABLE "test_db"."test_schema"."test_table" ("col_date" date, "col_ts" timestamptz, "col_bool" bool NOT NULL)"#;
        let result = runtime.create_item(&object_id, sql);
        assert!(
            result.is_ok(),
            "CREATE TABLE with date column failed: {:?}",
            result.err()
        );
    }

    #[mz_ore::test]
    fn test_stub_table_with_date_column() {
        let mut runtime = CatalogRuntime::new().expect("catalog creation should succeed");
        runtime.ensure_user_schema("test_db", "test_schema");
        let object_id = ObjectId {
            database: "test_db".into(),
            schema: "test_schema".into(),
            object: "test_table".into(),
        };
        let mut columns = BTreeMap::new();
        columns.insert(
            "col_date".to_string(),
            ColumnType {
                r#type: "date".into(),
                nullable: true,
                position: 0,
                comment: None,
            },
        );
        columns.insert(
            "col_ts".to_string(),
            ColumnType {
                r#type: "timestamptz".into(),
                nullable: false,
                position: 1,
                comment: None,
            },
        );
        let result = runtime.create_stub_table(&object_id, &columns);
        assert!(
            result.is_ok(),
            "stub table with date column failed: {:?}",
            result.err()
        );
    }
}
