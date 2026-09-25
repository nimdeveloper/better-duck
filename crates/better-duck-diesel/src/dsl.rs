//! DuckDB DSL prelude: extension traits, operators, and scalar functions to bring
//! into scope with `use better_duck_diesel::dsl::*;` (alongside `use diesel::prelude::*;`).

pub use crate::expressions::aggregates::general::*;
pub use crate::expressions::aggregates::statistical::*;
pub use crate::expressions::functions::conditional::*;
pub use crate::expressions::functions::numeric::*;
pub use crate::expressions::functions::string::*;
pub use crate::expressions::operators::comparison::DuckExpressionMethods;
pub use crate::expressions::operators::pattern::DuckTextExpressionMethods;
