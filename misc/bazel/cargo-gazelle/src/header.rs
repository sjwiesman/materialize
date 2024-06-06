// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Helpers to generate the header of a Bazel file.

use crate::rules::LoadStatement;
use crate::targets::RustTarget;

use super::ToBazelDefinition;
use std::collections::BTreeMap;

use std::fmt;

static CODE_GENERATED_HEADER: &str = "# Code generated by cargo-gazelle DO NOT EDIT";

/// Header to include on a BUILD.bazel file.
///
/// Includes special text to indicate this file is generated, and imports any necessary Rust rules.
///
/// TODO(parkmycar): This works for now but should surely be refactored.
#[derive(Debug)]
pub struct BazelHeader {
    loads: Vec<LoadStatement>,
}

impl BazelHeader {
    pub fn generate(targets: &[&dyn RustTarget]) -> Self {
        let x = targets
            .iter()
            .flat_map(|t| t.rules().into_iter())
            .map(|rule| (rule.module(), rule));

        let mut rules = BTreeMap::new();
        for (module, rule) in x {
            let entry = rules.entry(module).or_insert(Vec::new());
            entry.push(rule);
        }

        let loads = rules.into_iter().map(LoadStatement::from).collect();

        BazelHeader { loads }
    }
}

impl ToBazelDefinition for BazelHeader {
    fn format(&self, writer: &mut dyn fmt::Write) -> Result<(), fmt::Error> {
        writeln!(writer, "{CODE_GENERATED_HEADER}")?;
        writeln!(writer)?;
        writeln!(
            writer,
            r#"package(default_visibility = ["//visibility:public"])"#
        )?;
        writeln!(writer)?;

        // TODO(parkmcar): Handle differently named root repositories.
        writeln!(
            writer,
            r#"load("@crates_io//:defs.bzl", "aliases", "all_crate_deps")"#
        )?;

        for stmt in &self.loads {
            stmt.format(writer)?;
            writeln!(writer)?;
        }

        Ok(())
    }
}