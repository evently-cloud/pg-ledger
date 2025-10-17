import {Sql} from "postgres"


export async function loadUtilities(sql: Sql): Promise<Sql> {
  console.log("loading utilities...")
  await sql.file("./ddl/001_utilities.ddl")

  return sql
}
