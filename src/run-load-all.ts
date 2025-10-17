import {initClient} from "./init-client"
import {loadAll} from "./load-all"


console.log("loading all...")

const sql = initClient()

loadAll(sql)
  .then(() => sql.end())
