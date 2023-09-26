import { SDK_CONTAINER_TYPE } from "cohort_sdk_js"

export const isSdkError = (reason: string): boolean => {
    return (reason && reason.indexOf(`"_typ":"${ SDK_CONTAINER_TYPE }"`) > 0 && reason.startsWith("{") && reason.endsWith("}"))
}
