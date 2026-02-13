# dbt Fusion Extension Guide

A practical guide to using the dbt VS Code extension in this project. The extension is powered by the dbt Fusion engine and provides IDE features that make working with dbt models faster and less error-prone.

## Installation

The extension is already listed as a recommended extension in the project workspace file. When you open the project in VS Code, you should see a prompt to install recommended extensions. If not:

1. Open the Extensions panel (`Ctrl+Shift+X`)
2. Search for **dbt** by dbt Labs (`dbtLabsInc.dbt`)
3. Install it

The extension uses the dbt Fusion engine under the hood. It does not require dbt Core to be running — it parses your project independently.

## Key Features

### Hover Over `SELECT *`

When a model uses `SELECT *`, you can hover over the `*` to see the full list of columns and their data types that the star expands to. This is useful for understanding what a model actually outputs without having to trace back through its upstream dependencies.

Hovering over any column name or alias also shows its resolved data type.

This project discourages `SELECT *` in staging models (see [Modelling Guide](modelling-guide.md)), but it appears in raw models and occasionally in modelling or reporting layers. The hover feature helps you work with these models without guessing what columns are present.

### Column-Level Lineage

The extension can show lineage at the column level — not just which models depend on each other, but which specific columns flow through the DAG and where they originate.

**To view column lineage:**

1. Open a model file
2. Right-click on a column name or anywhere in the SQL
3. Select **dbt: View Lineage** then **Show column lineage**
4. Select the column you want to trace

The lineage view opens in a panel showing how that column flows upstream and downstream through the project. Double-click on any node to navigate to that model.

**Using column selectors in lineage:**

In the lineage panel, you can use the `column:` prefix to filter to a specific column. For example: `+column:model.ncl_analytics.dim_practice.practice_code+` shows everything upstream and downstream of that specific column.

This is particularly useful for impact analysis — before changing a column, you can see every model that uses it.

### Instant Refactoring: Rename Columns

One of the most useful features for this project. If you need to rename a column and it's referenced in downstream models, the extension can update all references automatically.

**To rename a column:**

1. Right-click on a column alias in your SQL
2. Select **Rename Symbol** (or press `F2`)
3. Type the new name
4. The extension shows a preview of every file that will be changed
5. Review the changes and confirm

All downstream `ref()` models that reference that column will be updated. This removes the risk of renaming a column in one model and breaking everything downstream.

**To rename a model:**

1. Right-click the model file in the Explorer
2. Select **Rename**
3. The extension asks if you want to update all `ref()` calls project-wide
4. Confirm to update all references

### Go-to-Definition

Jump directly to where something is defined, instead of searching the project manually.

**How to use it:**

- **`Ctrl+Click`** (or `Cmd+Click` on macOS) on any reference
- Or right-click and select **Go to Definition**

**Works with:**

| Element | What it does |
|---------|-------------|
| `ref('model_name')` | Opens the model's SQL file |
| `source('schema', 'table')` | Opens the source YAML definition |
| `{{ macro_name() }}` | Opens the macro's SQL file |
| Column names | Jumps to where the column is defined or aliased |
| CTE names | Jumps to the CTE definition within the same file |
| `*` in `SELECT *` | Shows the expanded column list |

This is especially helpful in the reporting and modelling layers where models reference multiple upstream dependencies.

### Live Error Detection

The extension validates your SQL as you type without querying Snowflake. It catches:

- **SQL syntax errors** — missing commas, misspelled keywords, unclosed brackets
- **Invalid column references** — referencing a column that doesn't exist in the upstream model
- **Missing GROUP BY columns** — selecting a non-aggregated column without grouping
- **Invalid ref/source calls** — referencing a model or source that doesn't exist
- **Invalid function arguments** — wrong number or type of arguments to SQL functions

Errors appear as red underlines in the editor and in the Problems panel (`Ctrl+Shift+M`).

**Note on false positives:** The extension may flag some YAML files with false positive errors. The project workspace is already configured to suppress error decorations in the Explorer to avoid visual noise from this. If you see YAML errors in the Problems panel that look incorrect, they can generally be ignored.

### IntelliSense and Autocomplete

The extension provides autocomplete suggestions as you type:

- **`ref('`** — lists all available models in the project
- **`source('`** — lists all configured sources
- **SQL functions** — suggests Snowflake-specific SQL functions
- **Column names** — suggests columns from referenced models after typing a table alias

This makes it easier to write correct SQL without having to look up model names or column names separately.

### Compiled Code View

See the actual SQL that dbt will execute, with all `ref()`, `source()`, and macro calls resolved.

**To view compiled SQL:**

Click the split-view code icon in the editor toolbar (or use the command palette: **dbt: Show Compiled SQL**). A side-by-side panel opens showing your dbt SQL on the left and the compiled output on the right.

The compiled view updates automatically when you save. This is useful for:

- Checking that `ref()` resolves to the correct table name
- Verifying macro output
- Copying compiled SQL to run directly in Snowflake for debugging

### CTE Previews

Preview the results of a CTE directly in the editor without running the full model.

**To preview a CTE:**

1. Place your cursor inside a CTE
2. Press `Ctrl+Enter` (`Cmd+Enter` on macOS)
3. Results appear in the Query Results tab

This requires a working Snowflake connection (run `.\start_dbt.ps1` first). It's useful for iterating on complex CTEs without having to build the full model each time.

### Lineage Panel

The Lineage tab in the sidebar shows the DAG for the currently open file — its upstream dependencies and downstream dependents.

- Click on any node to open that model
- Toggle between table-level and column-level lineage
- Use the search bar to filter to specific models or columns

This provides the same information as `dbt docs serve` but without leaving the editor.

## Recommended Workflow

1. **Write SQL with autocomplete** — let IntelliSense suggest model names, columns, and functions
2. **Check errors as you go** — fix red underlines before building
3. **Hover to explore** — hover over `*`, columns, and refs to understand what you're working with
4. **Use go-to-definition** — `Ctrl+Click` through the DAG instead of manually searching for files
5. **Preview CTEs** — validate intermediate logic with `Ctrl+Enter`
6. **Check compiled SQL** — verify the output before building
7. **Trace lineage before renaming** — use column lineage to understand the impact of changes
8. **Rename with confidence** — use Rename Symbol to update columns across the project safely

## Troubleshooting

**Extension not activating:**
- Ensure the extension is installed (`dbtLabsInc.dbt`)
- Check that the project has a `dbt_project.yml` in the workspace root

**No autocomplete or hover:**
- Wait for the Fusion engine to finish parsing (check the status bar for progress)
- Large projects take a moment to index on first open

**CTE preview not working:**
- Run `.\start_dbt.ps1` to load your Snowflake credentials
- Check `dbt debug` to verify your connection

**YAML false positive errors:**
- These are a known limitation. The workspace config suppresses error decorations in the Explorer. You can also filter the Problems panel to show only warnings and above

## Further Reading

- [dbt VS Code Extension Features](https://docs.getdbt.com/docs/dbt-extension-features) — official feature documentation
- [Install and Configure the Extension](https://docs.getdbt.com/docs/install-dbt-extension) — setup guide from dbt Labs
- [Fusion Engine Quickstart](https://docs.getdbt.com/guides/fusion) — getting started with Fusion
