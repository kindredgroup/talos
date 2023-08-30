import { PoolConfig } from "pg"

const DB_CONFIG: PoolConfig = {
    application_name: "cohort_banking_js",
    keepAlive: true,
    host: "127.0.0.1",
    port: 5432,
    database: "talos-sample-cohort-dev",
    user: "postgres",
    password: "admin",
    max: 20,
    min: 20,
}

export { DB_CONFIG }