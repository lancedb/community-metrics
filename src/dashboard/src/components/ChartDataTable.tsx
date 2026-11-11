type ChartDataTableColumn<Row> = {
  label: string
  value: (row: Row) => string | number
}

type ChartDataTableProps<Row> = {
  caption: string
  columns: ChartDataTableColumn<Row>[]
  rows: Row[]
}

function formatCell(value: string | number): string {
  return typeof value === 'number' ? Intl.NumberFormat('en-US').format(value) : value
}

/**
 * Screen-reader-only table of exactly the points a chart plots.
 *
 * Charts are SVG, so their numbers are otherwise invisible to assistive tech. The UI
 * tests (tests/dashboard.spec.ts) also read these tables to check what each chart shows,
 * so every chart must keep rendering one, whatever library draws it.
 *
 * The sr-only class goes on a wrapper, not the table: a table never shrinks below its
 * rows, so a hidden table with hundreds of rows would still add blank page height.
 */
export function ChartDataTable<Row>({ caption, columns, rows }: ChartDataTableProps<Row>) {
  return (
    <div className="sr-only">
      <table data-chart-table={caption}>
        <caption>{caption}</caption>
        <thead>
          <tr>
            {columns.map((column) => (
              <th key={column.label} scope="col">
                {column.label}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, rowIndex) => (
            <tr key={rowIndex}>
              {columns.map((column) => (
                <td key={column.label}>{formatCell(column.value(row))}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
