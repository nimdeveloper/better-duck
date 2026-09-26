## Default Permission

Read access to loaded DuckDB databases: open/close a connection and run read-only
`select` queries. Writes (`execute`) and raw arbitrary SQL are NOT included here and
must be granted explicitly (e.g. `duck:allow-execute`, `duck:allow-raw-sql`).

#### This default permission set includes the following:

- `allow-load`
- `allow-close`
- `allow-select`

## Permission Table

<table>
<tr>
<th>Identifier</th>
<th>Description</th>
</tr>


<tr>
<td>

`better-duck-tauri:allow-close`

</td>
<td>

Enables the close command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`better-duck-tauri:deny-close`

</td>
<td>

Denies the close command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`better-duck-tauri:allow-execute`

</td>
<td>

Enables the execute command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`better-duck-tauri:deny-execute`

</td>
<td>

Denies the execute command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`better-duck-tauri:allow-load`

</td>
<td>

Enables the load command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`better-duck-tauri:deny-load`

</td>
<td>

Denies the load command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`better-duck-tauri:allow-select`

</td>
<td>

Enables the select command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`better-duck-tauri:deny-select`

</td>
<td>

Denies the select command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`better-duck-tauri:allow-raw-sql`

</td>
<td>

Allows running arbitrary SQL through the `select` and `execute` commands.

</td>
</tr>
</table>
