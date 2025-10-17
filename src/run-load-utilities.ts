import {initClient} from "./init-client"
import {loadUtilities} from "./load-utilities"


const sql = initClient()
loadUtilities(sql)
  .then((s) => s.end())
