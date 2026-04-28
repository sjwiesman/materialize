// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Constraint enforcement validation.
//!
//! Validates that constraint enforcement rules are respected based on the
//! parent object type they are defined on.

use crate::project::error::{ValidationError, ValidationErrorKind};
use crate::project::ir::compiled::FullyQualifiedName;
use mz_sql_parser::ast::*;

/// Validates constraint enforcement rules based on the parent object type.
///
/// Enforcement rules:
/// - **Constraints NOT allowed on:** `Table` (includes table-from-source)
/// - **Enforced is NOT allowed on:** `View` (error if `enforced: true` on a view)
/// - **Allowed:** `MaterializedView` (enforced or not), `View` (not-enforced only)
pub(super) fn validate_constraint_enforcement(
    fqn: &FullyQualifiedName,
    constraints: &[CreateConstraintStatement<Raw>],
    offsets: &[usize],
    obj_type: ObjectType,
    errors: &mut Vec<ValidationError>,
) {
    for (i, constraint) in constraints.iter().enumerate() {
        let offset = offsets[i];
        let constraint_sql = format!("{};", constraint);
        let constraint_name = constraint
            .name
            .as_ref()
            .map(|n| n.to_string())
            .unwrap_or_else(|| "<unnamed>".to_string());

        // Constraints are not allowed on tables (includes table-from-source)
        if matches!(obj_type, ObjectType::Table) {
            errors.push(ValidationError::with_file_sql_and_offset(
                ValidationErrorKind::ConstraintNotAllowedOnTable {
                    constraint_name,
                    object_type: "table".to_string(),
                },
                fqn.path.clone(),
                constraint_sql,
                offset,
            ));
            continue;
        }

        // Enforced constraints are not allowed on views
        if constraint.enforced && matches!(obj_type, ObjectType::View) {
            errors.push(ValidationError::with_file_sql_and_offset(
                ValidationErrorKind::EnforcedConstraintNotAllowed {
                    constraint_name,
                    object_type: "view".to_string(),
                },
                fqn.path.clone(),
                constraint_sql,
                offset,
            ));
        }
    }
}
