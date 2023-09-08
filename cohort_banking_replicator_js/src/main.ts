import { Pool } from "pg"

import { logger } from "./logger"
import { ReplicatorApp } from "./replicator-app"
import { DB_CONFIG } from "./cfg/config-db-pool"

new Promise(async (resolve) => {
    const database = new Pool(DB_CONFIG)
    database.on("error",   (e, _) => { logger.error("DBPool.error: Error: %s", e) })
    database.on("release", (e, _) => { if (e) { logger.error("DBPool.release: Error: %s", e) } })

    const c = await database.connect()
    await c.query("SELECT 1 as test")
    c.release()
    
    const app = new ReplicatorApp()
    await app.run()
})