import { describe, expect, test } from "bun:test"
import { ACP } from "../../src/acp/agent"
import type { AgentSideConnection } from "@agentclientprotocol/sdk"
import type { Event } from "@opencode-ai/sdk/v2"
import { Instance } from "../../src/project/instance"
import { tmpdir } from "../fixture/fixture"

type SessionUpdateParams = Parameters<AgentSideConnection["sessionUpdate"]>[0]
type RequestPermissionResult = Awaited<ReturnType<AgentSideConnection["requestPermission"]>>

type GlobalEventEnvelope = {
  directory?: string
  payload?: Event
}

type EventController = {
  push: (event: GlobalEventEnvelope) => void
  close: () => void
}

function createEventStream() {
  const queue: GlobalEventEnvelope[] = []
  const waiters: Array<(value: GlobalEventEnvelope | undefined) => void> = []
  const state = { closed: false }

  const push = (event: GlobalEventEnvelope) => {
    const waiter = waiters.shift()
    if (waiter) {
      waiter(event)
      return
    }
    queue.push(event)
  }

  const close = () => {
    state.closed = true
    for (const waiter of waiters.splice(0)) {
      waiter(undefined)
    }
  }

  const stream = async function* (signal?: AbortSignal) {
    while (true) {
      if (signal?.aborted) return
      const next = queue.shift()
      if (next) {
        yield next
        continue
      }
      if (state.closed) return
      const value = await new Promise<GlobalEventEnvelope | undefined>((resolve) => {
        waiters.push(resolve)
        if (!signal) return
        signal.addEventListener("abort", () => resolve(undefined), { once: true })
      })
      if (!value) return
      yield value
    }
  }

  return { controller: { push, close } satisfies EventController, stream }
}

function assistantMessageEvent(
  sessionID: string,
  opts: {
    id: string
    parentID: string
    cwd: string
    completed?: boolean
    finish?: string
    tokens?: { input: number; output: number }
  },
): GlobalEventEnvelope {
  return {
    payload: {
      type: "message.updated",
      properties: {
        info: {
          id: opts.id,
          sessionID,
          role: "assistant",
          parentID: opts.parentID,
          time: {
            created: Date.now(),
            ...(opts.completed && { completed: Date.now() }),
          },
          ...(opts.finish && { finish: opts.finish }),
          tokens: {
            input: opts.tokens?.input ?? 0,
            output: opts.tokens?.output ?? 0,
            reasoning: 0,
            cache: { read: 0, write: 0 },
          },
          cost: 0,
          modelID: "big-pickle",
          providerID: "opencode",
          mode: "agent",
          agent: "build",
          path: { cwd: opts.cwd, root: opts.cwd },
        },
      },
    },
  } as any
}

type PromptCall = {
  params: any
  resolve: (value: any) => void
  reject: (error: any) => void
}

function createPromptQueueAgent() {
  const sessionUpdates: SessionUpdateParams[] = []
  const promptCalls: PromptCall[] = []

  const connection = {
    async sessionUpdate(params: SessionUpdateParams) {
      sessionUpdates.push(params)
    },
    async requestPermission(): Promise<RequestPermissionResult> {
      return { outcome: { outcome: "selected", optionId: "once" } } as RequestPermissionResult
    },
  } as unknown as AgentSideConnection

  const { controller, stream } = createEventStream()
  const calls = { eventSubscribe: 0, sessionCreate: 0 }

  const sdk = {
    global: {
      event: async (opts?: { signal?: AbortSignal }) => {
        calls.eventSubscribe++
        return { stream: stream(opts?.signal) }
      },
    },
    session: {
      create: async (_params?: any) => {
        calls.sessionCreate++
        return {
          data: {
            id: `ses_${calls.sessionCreate}`,
            time: { created: new Date().toISOString() },
          },
        }
      },
      get: async (_params?: any) => ({
        data: { id: "ses_1", time: { created: new Date().toISOString() } },
      }),
      messages: async () => ({ data: [] }),
      message: async (params?: any) => ({
        data: {
          info: { role: "assistant" },
          parts: [{ id: params?.messageID ? `${params.messageID}_part` : "part_1", type: "text", text: "" }],
        },
      }),
      prompt: (params: any) => {
        return new Promise((resolve, reject) => {
          promptCalls.push({ params, resolve, reject })
        })
      },
    },
    permission: { respond: async () => ({ data: true }) },
    config: {
      providers: async () => ({
        data: { providers: [{ id: "opencode", name: "opencode", models: { "big-pickle": { id: "big-pickle", name: "big-pickle" } } }] },
      }),
    },
    app: {
      agents: async () => ({
        data: [{ name: "build", description: "build", mode: "agent" }],
      }),
    },
    command: { list: async () => ({ data: [] }) },
    mcp: { add: async () => ({ data: true }) },
  } as any

  const agent = new ACP.Agent(connection, {
    sdk,
    defaultModel: { providerID: "opencode", modelID: "big-pickle" },
  } as any)

  const stop = () => {
    controller.close()
    ;(agent as any).eventAbort.abort()
  }

  async function createSession(cwd: string) {
    const sessionId = await agent.newSession({ cwd, mcpServers: [] } as any).then((x) => x.sessionId)
    ;(agent as any).sessionManager.setMode(sessionId, "build")
    return sessionId
  }

  function startPrompt(sessionId: string, text: string) {
    let resolved = false
    let result: any
    const promise = agent
      .prompt({ sessionId, prompt: [{ type: "text", text }] } as any)
      .then((r) => {
        resolved = true
        result = r
        return r
      })
    return {
      promise,
      get resolved() {
        return resolved
      },
      get result() {
        return result
      },
    }
  }

  function resolvePromptCall(index: number, tokens?: { input: number; output: number }) {
    promptCalls[index].resolve({
      data: {
        info: {
          role: "assistant",
          tokens: {
            input: tokens?.input ?? 0,
            output: tokens?.output ?? 0,
            reasoning: 0,
            cache: { read: 0, write: 0 },
          },
          cost: 0,
        },
      },
    })
  }

  return { agent, controller, promptCalls, sessionUpdates, stop, createSession, startPrompt, resolvePromptCall }
}

const tick = (ms = 10) => new Promise((r) => setTimeout(r, ms))

describe("acp.agent prompt queueing", () => {
  test("single prompt resolves via SDK fallback", async () => {
    await using tmp = await tmpdir()
    await Instance.provide({
      directory: tmp.path,
      fn: async () => {
        const { stop, createSession, startPrompt, resolvePromptCall } = createPromptQueueAgent()
        const sessionId = await createSession("/tmp/test")

        const p1 = startPrompt(sessionId, "hello")
        await tick()

        expect(p1.resolved).toBe(false)

        resolvePromptCall(0, { input: 50, output: 25 })
        await p1.promise

        expect(p1.resolved).toBe(true)
        expect(p1.result.stopReason).toBe("end_turn")
        expect(p1.result.usage?.inputTokens).toBe(50)

        stop()
      },
    })
  })

  test("previous prompt resolves early when newer message enters context", async () => {
    await using tmp = await tmpdir()
    await Instance.provide({
      directory: tmp.path,
      fn: async () => {
        const { controller, promptCalls, stop, createSession, startPrompt, resolvePromptCall } =
          createPromptQueueAgent()
        const cwd = "/tmp/test"
        const sessionId = await createSession(cwd)

        const p1 = startPrompt(sessionId, "first")
        await tick()
        const p1MessageID = promptCalls[0].params.messageID

        const p2 = startPrompt(sessionId, "second")
        await tick()
        const p2MessageID = promptCalls[1].params.messageID

        expect(p1.resolved).toBe(false)
        expect(p2.resolved).toBe(false)

        // Assistant response to first prompt completes — but first prompt should
        // NOT resolve yet because the second message hasn't entered context
        controller.push(
          assistantMessageEvent(sessionId, {
            id: "asst_1",
            parentID: p1MessageID,
            cwd,
            completed: true,
            finish: "end_turn",
            tokens: { input: 100, output: 50 },
          }),
        )
        await tick()
        expect(p1.resolved).toBe(false)

        // Assistant response to second prompt is created — this means the
        // second message has entered context, so the first prompt resolves
        controller.push(
          assistantMessageEvent(sessionId, {
            id: "asst_2",
            parentID: p2MessageID,
            cwd,
          }),
        )
        await tick()

        expect(p1.resolved).toBe(true)
        expect(p1.result.stopReason).toBe("end_turn")
        expect(p1.result.usage?.inputTokens).toBe(100)
        expect(p2.resolved).toBe(false)

        // Resolve P2 via SDK fallback
        resolvePromptCall(1, { input: 200, output: 100 })
        await p2.promise

        expect(p2.resolved).toBe(true)
        expect(p2.result.stopReason).toBe("end_turn")

        stop()
      },
    })
  })

  test("early-resolved prompt includes usage from completed assistant message", async () => {
    await using tmp = await tmpdir()
    await Instance.provide({
      directory: tmp.path,
      fn: async () => {
        const { controller, promptCalls, stop, createSession, startPrompt } = createPromptQueueAgent()
        const cwd = "/tmp/test"
        const sessionId = await createSession(cwd)

        const p1 = startPrompt(sessionId, "first")
        await tick()
        const p1MessageID = promptCalls[0].params.messageID

        const p2 = startPrompt(sessionId, "second")
        await tick()
        const p2MessageID = promptCalls[1].params.messageID

        // First prompt's response completes with specific token counts
        controller.push(
          assistantMessageEvent(sessionId, {
            id: "asst_1",
            parentID: p1MessageID,
            cwd,
            completed: true,
            finish: "end_turn",
            tokens: { input: 1234, output: 567 },
          }),
        )
        await tick()

        // Second prompt's response is created — triggers first prompt resolution
        controller.push(
          assistantMessageEvent(sessionId, {
            id: "asst_2",
            parentID: p2MessageID,
            cwd,
          }),
        )
        await tick()

        expect(p1.result.usage).toBeDefined()
        expect(p1.result.usage.inputTokens).toBe(1234)
        expect(p1.result.usage.outputTokens).toBe(567)
        expect(p1.result.usage.totalTokens).toBe(1234 + 567)

        stop()
      },
    })
  })

  test("tool-calls finish does not trigger early resolution", async () => {
    await using tmp = await tmpdir()
    await Instance.provide({
      directory: tmp.path,
      fn: async () => {
        const { controller, promptCalls, stop, createSession, startPrompt } = createPromptQueueAgent()
        const cwd = "/tmp/test"
        const sessionId = await createSession(cwd)

        const p1 = startPrompt(sessionId, "first")
        await tick()
        const p1MessageID = promptCalls[0].params.messageID

        // Assistant message with finish=tool-calls (loop continues, same user message)
        controller.push(
          assistantMessageEvent(sessionId, {
            id: "asst_1a",
            parentID: p1MessageID,
            cwd,
            completed: true,
            finish: "tool-calls",
          }),
        )
        await tick()

        // Another assistant message for same user message (after tool execution)
        controller.push(
          assistantMessageEvent(sessionId, {
            id: "asst_1b",
            parentID: p1MessageID,
            cwd,
          }),
        )
        await tick()

        // P1 should not be resolved — all messages have the same parentID
        expect(p1.resolved).toBe(false)

        stop()
      },
    })
  })
})
