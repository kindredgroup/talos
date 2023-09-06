import { createLogger, format, transports } from "winston"

const logger = createLogger({
    level: "info",
    format: format.combine(
        format.colorize(),
        format.timestamp(),
        format.align(),
        format.splat(),
        format.simple()
    ),
    // defaultMeta: { service: "cohort_banking_js" },
    transports: [
        new transports.Console()
    ],
})

export { logger }