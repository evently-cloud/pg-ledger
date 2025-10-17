import {Sql} from "postgres"


export async function loadEventFunctions(sql: Sql): Promise<Sql> {
  console.log("loading event functions...")
  await sql.file("./ddl/004_events.ddl")

  return sql
}
