import {Sql} from "postgres"


const typesDdl = "./ddl/000_types.ddl"
const utilsDdl = "./ddl/001_utilities.ddl"


export async function loadShared(sql: Sql): Promise<Sql> {
  console.log("loading shared types and utilities...")
  await sql.file(typesDdl)
  await sql.file(utilsDdl)

  return sql
}
