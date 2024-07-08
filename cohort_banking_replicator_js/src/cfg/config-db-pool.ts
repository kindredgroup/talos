import { PoolConfig } from "pg"

const getParam = (param: string, defaultValue: string): string => {
    const v = process.env[param]
    if (v) return v
    return defaultValue
}

const DB_CONFIG: PoolConfig = {
    application_name: "cohort_banking_replicator_js",
    keepAlive:  true,
    host:       getParam("COHORT_PG_HOST", "127.0.0.1"),
    port:       parseInt(getParam("COHORT_PG_PORT", "5432")),
    database:   getParam("COHORT_PG_DATABASE", "talos-sample-cohort-dev"),
    user:       getParam("COHORT_PG_USER", "postgres"),
    password:   getParam("COHORT_PG_PASSWORD", "admin"),
    max:        parseInt(getParam("COHORT_PG_POOL_SIZE", "100")),
    min:        parseInt(getParam("COHORT_PG_POOL_SIZE", "100")),
}

export { DB_CONFIG }