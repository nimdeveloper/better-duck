//! Diesel `Row` and `Field` implementations for DuckDB.
//!
//! `Row` owns its [`DuckRow`] data and shares column names via [`Arc`] with the
//! cursor that produced it. This avoids any self-referential lifetimes and the
//! unsound `&'a DuckResult<'a>` pattern.

use std::{marker::PhantomData, ops::Range, sync::Arc};

use better_duck_core::{types::value_ref::DuckValueRef, DuckRow};
use diesel::row::{Field as DieselField, PartialRow, Row as DieselRow, RowIndex, RowSealed};

use crate::backend::DuckDb;

// Field

/// A single field in a DuckDB result row.
pub struct Field<'f> {
    /// Column name borrowed from the `Arc`-shared name slice in the owning [`Row`].
    name: &'f str,
    /// The field value as a [`DuckValueRef`].
    value: DuckValueRef<'f>,
}

impl<'f> DieselField<'f, DuckDb> for Field<'f> {
    fn field_name(&self) -> Option<&str> {
        Some(self.name)
    }

    /// Returns `None` when the field is `NULL`, `Some(value)` otherwise.
    ///
    /// Diesel's `Field` contract requires `value()` to return `None` for NULL
    /// values so that `from_nullable_sql(None)` is called and `Option<T>`
    /// correctly resolves to `Ok(None)`.  Returning `Some(DuckValueRef::Null)`
    /// for NULL would bypass that path and propagate the `Null` variant into
    /// concrete `FromSql` impls, causing spurious deserialization errors.
    fn value(&self) -> Option<DuckValueRef<'_>> {
        match &self.value {
            DuckValueRef::Null => None,
            v => Some(v.clone()),
        }
    }

    fn is_null(&self) -> bool {
        matches!(self.value, DuckValueRef::Null)
    }
}

// Row

/// A single row from a DuckDB result set.
///
/// The lifetime parameter `'conn` ties the row to the connection that produced
/// it, ensuring the row cannot outlive the cursor (and by extension the
/// connection).
pub struct Row<'conn> {
    inner: DuckRow,
    col_names: Arc<[Box<str>]>,
    _marker: PhantomData<&'conn ()>,
}

impl<'conn> Row<'conn> {
    /// Creates a new `Row` from a [`DuckRow`] and a shared column-name slice.
    pub(crate) fn new(
        inner: DuckRow,
        col_names: Arc<[Box<str>]>,
    ) -> Self {
        Row { inner, col_names, _marker: PhantomData }
    }
}

impl<'conn> DieselRow<'conn, DuckDb> for Row<'conn> {
    type Field<'f>
        = Field<'f>
    where
        'conn: 'f,
        Self: 'f;

    type InnerPartialRow = Self;

    fn field_count(&self) -> usize {
        self.inner.column_count() as usize
    }

    fn partial_row(
        &self,
        range: Range<usize>,
    ) -> PartialRow<'_, Self> {
        PartialRow::new(self, range)
    }

    fn get<'b, I>(
        &'b self,
        idx: I,
    ) -> Option<Self::Field<'b>>
    where
        'conn: 'b,
        Self: RowIndex<I>,
    {
        let i = self.idx(idx)?;
        let name = self.col_names.get(i)?.as_ref();
        let value = self.inner.get_idx(i).map(DuckValueRef::from)?;
        Some(Field { name, value })
    }
}

impl RowIndex<usize> for Row<'_> {
    fn idx(
        &self,
        i: usize,
    ) -> Option<usize> {
        (i < self.inner.column_count() as usize).then_some(i)
    }
}

impl<'b> RowIndex<&'b str> for Row<'_> {
    fn idx(
        &self,
        name: &'b str,
    ) -> Option<usize> {
        self.col_names.iter().position(|n| n.as_ref() == name)
    }
}

impl RowSealed for Row<'_> {}
