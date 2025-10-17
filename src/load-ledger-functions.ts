import {Sql} from "postgres"


export async function loadLedgerFunctions(sql: Sql): Promise<Sql> {
  console.log("loading ledger functions...")
  await sql.file("./ddl/003_ledgers_functions.ddl")

  return sql
}
