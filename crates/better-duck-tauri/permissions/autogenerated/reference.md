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

`better-duck-tauri:allow-append-rows`

</td>
<td>

Enables the append_rows command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`better-duck-tauri:deny-append-rows`

</td>
<td>

Denies the append_rows command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`better-duck-tauri:allow-checkpoint`

</td>
<td>

Enables the checkpoint command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`better-duck-tauri:deny-checkpoint`

</td>
<td>

Denies the checkpoint command without any pre-configured scope.

</td>
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

`better-duck-tauri:allow-columns`

</td>
<td>

Enables the columns command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`better-duck-tauri:deny-columns`

</td>
<td>

Denies the columns command without any pre-configured scope.

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

`better-duck-tauri:allow-explain`

</td>
<td>

Enables the explain command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`better-duck-tauri:deny-explain`

</td>
<td>

Denies the explain command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`better-duck-tauri:allow-export`

</td>
<td>

Enables the export command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`better-duck-tauri:deny-export`

</td>
<td>

Denies the export command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`better-duck-tauri:allow-import`

</td>
<td>

Enables the import command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`better-duck-tauri:deny-import`

</td>
<td>

Denies the import command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`better-duck-tauri:allow-interrupt`

</td>
<td>

Enables the interrupt command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`better-duck-tauri:deny-interrupt`

</td>
<td>

Denies the interrupt command without any pre-configured scope.

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

`better-duck-tauri:allow-load-extension`

</td>
<td>

Enables the load_extension command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`better-duck-tauri:deny-load-extension`

</td>
<td>

Denies the load_extension command without any pre-configured scope.

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

`better-duck-tauri:allow-stream`

</td>
<td>

Enables the stream command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`better-duck-tauri:deny-stream`

</td>
<td>

Denies the stream command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`better-duck-tauri:allow-tables`

</td>
<td>

Enables the tables command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`better-duck-tauri:deny-tables`

</td>
<td>

Denies the tables command without any pre-configured scope.

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
