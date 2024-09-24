pub mod cli;

use anyhow::Result;

pub type SummarizeFn = fn(&std::path::Path, Option<usize>, usize) -> Result<String>;

// Macro to create list of `summarize` functions found in modules
macro_rules! summarize_functions {
    ($($module:ident),*) => {
        // Import modules
        $(pub mod $module;)*

        pub fn versions() -> Vec<SummarizeFn> {
            vec![$($module::summarize),*]
        }
    };
}

summarize_functions!(v0, v1, v2, v3);
