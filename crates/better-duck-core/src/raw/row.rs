use crate::types::value::DuckValue;

/// Implementation of methods for the `AbstractRow` struct.
///
/// Provides functionality to create a new row from a vector of `DuckValue`s and a list of column names,
/// as well as to retrieve a value by its column name.
///
/// # Methods
///
/// - `new(result: Vec<DuckValue>, col_names: Box<[&'static str]>) -> Self`  
///   Constructs a new `AbstractRow` from the given values and column names.
///
/// - `get(&self, name: &str) -> Option<&DuckValue>`  
///   Retrieves a reference to the value associated with the specified column name, if it exists.
#[derive(Debug)]
pub struct AbstractRow(Vec<DuckValue>, Box<[&'static str]>);

impl AbstractRow {
    /// Creates a new `AbstractRow` instance from a vector of `DuckValue` and a boxed slice of column names.
    ///
    /// # Arguments
    ///
    /// * `result` - A vector containing the values for each column in the row.
    /// * `col_names` - A boxed slice of static string slices representing the column names.
    ///
    /// # Returns
    ///
    /// A new `AbstractRow` containing the provided values and column names.
    pub fn new(
        result: Vec<DuckValue>,
        col_names: Box<[&'static str]>,
    ) -> Self {
        AbstractRow(result, col_names)
    }

    /// Retrieves a reference to the value associated with the specified column name.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the column to retrieve the value for.
    ///
    /// # Returns
    ///
    /// An `Option` containing a reference to the `DuckValue` if the column exists, or `None` if it does not.
    #[allow(unused)]
    pub fn get(
        &self,
        name: &str,
    ) -> Option<&DuckValue> {
        for (i, each) in self.1.iter().enumerate() {
            if each == &name {
                return Some(&self.0[i]);
            }
        }
        None
    }
}
