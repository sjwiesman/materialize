// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{error::Error, fmt::Display};

use terminal_menu::{button, label, menu, mut_menu, run};

const QUIT: &str = "quit";

#[derive(Debug)]
pub enum AskError {
    EmptyChoices,
}

impl Display for AskError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AskError::EmptyChoices => write!(f, "No choices provided to ask"),
        }
    }
}

impl Error for AskError {}

/// Asks the user to select 1 of several choices.
///
/// - If the choices list is empty the function will retun with an error
/// - If the choices list contains a single item, it will be returned immediatly
///   without prompting the user
/// - Otherwise, the user will be prompted to make some selection which is returned,
///   or none if declined
pub fn prompt_user(header: &str, mut choices: Vec<String>) -> Result<Option<String>, AskError> {
    if choices.is_empty() {
        return Err(AskError::EmptyChoices);
    }

    if choices.len() == 1 {
        return Ok(choices.pop());
    }

    let mut elements = Vec::new();
    elements.push(label(header));
    elements.push(label(underline_header(header)));

    elements.append(&mut choices.into_iter().map(button).collect());
    elements.push(button(QUIT));

    let menu = menu(elements);
    run(&menu);
    let selection = {
        let binding = mut_menu(&menu);
        binding.selected_item_name().to_string()
    };

    if selection == QUIT {
        Ok(None)
    } else {
        Ok(Some(selection))
    }
}

fn underline_header(header: &str) -> String {
    header.chars().map(|_| '-').collect()
}
