//! Result metadata: change counts, statement/result classification, column info.

use better_duck_core::connection::Connection;
use common::{section, show, Result};

fn main() -> Result<()> {
    let mut conn = Connection::open_in_memory()?;
    conn.execute_batch("CREATE TABLE t (v INTEGER)")?;

    section("DML: changes() reports affected rows");
    let mut insert = conn.execute("INSERT INTO t VALUES (1), (2), (3)")?;
    show("changes", insert.changes());
    show("statement_type", insert.statement_type());
    show("result_type", insert.result_type());
    assert_eq!(insert.changes(), 3);

    section("SELECT: column metadata");
    let select = conn.execute("SELECT 1 AS a, 'x' AS b, 3.5 AS c")?;
    show("column_count", select.column_count());
    show("column_name(0)", select.column_name(0)?);
    show("column_idx(\"b\")", select.column_idx("b"));
    show("column_type(0)", select.column_type(0)?);
    show("column_schema", select.column_schema().to_vec());
    assert_eq!(select.column_count(), 3);
    assert_eq!(select.column_name(1)?, "b");
    assert_eq!(select.column_idx("c"), Some(2));

    Ok(())
}
