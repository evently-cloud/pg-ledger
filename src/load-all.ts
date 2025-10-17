import {Sql} from "postgres"
import {loadShared} from "./load-shared"
import {loadLedgerFunctions} from "./load-ledger-functions"
import {loadEventFunctions} from "./load-event-functions"
import {loadLedgerTables} from "./load-ledger-tables"
import {loadNotifiers} from "./load-notifiers"


export async function loadAll(sql: Sql): Promise<Sql> {
  await sql.begin(async (tsql) => {
    await loadShared(tsql)
    await loadLedgerTables(tsql)
    await loadLedgerFunctions(tsql)
    await loadEventFunctions(tsql)
    await loadNotifiers(tsql)
  })
  return sql
}
