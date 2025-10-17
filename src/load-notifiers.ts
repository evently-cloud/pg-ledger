import {Sql} from "postgres"


export async function loadNotifiers(sql: Sql): Promise<Sql> {
  console.log("loading notifiers...")
  await sql.file("./ddl/005_notifiers.ddl")

  return sql
}
