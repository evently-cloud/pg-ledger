import {Sql} from "postgres"


export async function loadLedgerTables(sql: Sql): Promise<Sql> {
  console.log("loading ledger tables...")
  await sql.file("./ddl/002_ledgers_tables.ddl")

  return sql
}
