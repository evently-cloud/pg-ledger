import env from "env-sanitize"
import {omitBy} from "lodash"
import PG, {Options, Sql} from "postgres"


export function initClient(): Sql {
  let ssl
  if (env("PGSSL", (e) => e.asBoolean(), false)) {
    ssl = {
      rejectUnauthorized: false
    }
  }

  const typeOpts = {
    max: 1,
    debug: true,
    connect_timeout: 1,
    ssl,
    types: {
      bigint: PG.BigInt
    }
  }
  const config = gatherConfig(typeOpts)

  return typeof config === "string"
    ? PG(config, typeOpts)
    : PG(config)

}


function gatherConfig(typeOpts: Options<any>): string | Options<any> {
  const url = env("DATABASE_URL", false)
  if (url) {
    return url
  }

  const dbPrefix = env("DB_PREFIX", "DB")  // "RDS" for Amazon
  const database = env(`${dbPrefix}_DATABASE`, "evently")
  const user = env(`${dbPrefix}_USER`, "")
  const password = env(`${dbPrefix}_PASSWORD`, "")
  const host = env(`${dbPrefix}_HOST`, "")
  const port = env(`${dbPrefix}_PORT`, (e) => e.asInt(), 5432)

  const envConfig = {
    ...typeOpts,
    database,
    user,
    password,
    host,
    port
  }

  //pg checks for the existence of keys, so take out the undefined keys.
  const all = omitBy(envConfig, (v) => !v)
  console.info(JSON.stringify(all, null, 2))
  return all
}
