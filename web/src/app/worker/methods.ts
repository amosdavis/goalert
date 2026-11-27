/* eslint-disable @typescript-eslint/no-explicit-any */
const methods: Record<string, (arg: any) => any> = {}
export default methods

export type WorkerMethodName = keyof typeof methods
export type WorkerMethod<N extends WorkerMethodName> = (typeof methods)[N]
export type WorkerResult<N extends WorkerMethodName> = ReturnType<
  WorkerMethod<N>
>
export type WorkerReturnType<N extends WorkerMethodName> = [
  result: WorkerResult<N>,
  status: { loading: boolean },
]
export type WorkerParam<N extends WorkerMethodName> = Parameters<
  WorkerMethod<N>
>[0]
