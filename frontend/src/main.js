import { Terminal } from "@xterm/xterm";
import { FitAddon } from "@xterm/addon-fit";
import "@xterm/xterm/css/xterm.css";
import "./styles.css";

const state = {
  threads: [],
  selected: null,
  terminalSocket: null,
  terminalThreadId: null,
  terminalConnectPromise: null,
  terminalGeneration: 0,
  terminalInputSequence: 0,
  terminalPendingAcks: new Map(),
  eventsSocket: null,
  eventsReconnectTimer: null,
  eventsReconnectAttempt: 0,
  eventProcessing: Promise.resolve(),
  lastEventId: 0,
  refreshTimer: null,
  refreshing: false,
  refreshPending: false,
};

const $ = (selector) => document.querySelector(selector);
const activityElement = $("#activity-list");

function log(label, value = "", level = "info") {
  const item = document.createElement("article");
  item.className = `activity-item ${level}`;
  const header = document.createElement("header");
  const title = document.createElement("strong");
  title.textContent = label;
  const time = document.createElement("time");
  time.dateTime = new Date().toISOString();
  time.textContent = new Date().toLocaleTimeString();
  header.append(title, time);
  item.append(header);
  if (value !== "" && value !== undefined) {
    const detail = document.createElement("pre");
    detail.textContent =
      typeof value === "string" ? value : JSON.stringify(value, null, 2);
    item.append(detail);
  }
  activityElement.prepend(item);
  while (activityElement.children.length > 200) {
    activityElement.lastElementChild.remove();
  }
}

function setEventStatus(label, stateName = "") {
  const status = $("#event-status");
  status.textContent = label;
  status.className = `event-status ${stateName}`.trim();
}

function eventLabel(event) {
  const subject =
    event.thread_id ||
    event.thread?.id ||
    event.item?.thread_id ||
    "";
  return `${event.type}${subject ? ` · ${String(subject).slice(0, 12)}` : ""}`;
}

function scheduleRefresh() {
  if (state.refreshTimer !== null) return;
  state.refreshTimer = window.setTimeout(() => {
    state.refreshTimer = null;
    void refresh();
  }, 80);
}

async function api(path, options = {}) {
  const response = await fetch(path, {
    ...options,
    credentials: "same-origin",
    headers: { "Content-Type": "application/json", ...(options.headers || {}) },
  });
  const payload = await response.json();
  if (!response.ok) {
    throw new Error(payload.error?.message || `HTTP ${response.status}`);
  }
  return payload;
}

const terminal = new Terminal({
  cursorBlink: true,
  convertEol: false,
  fontFamily:
    '"SFMono-Regular", "SF Mono", Menlo, Monaco, Consolas, "Liberation Mono", monospace',
  fontSize: 13,
  lineHeight: 1,
  theme: {
    background: "#090b0d",
    foreground: "#e5e7eb",
    cursor: "#7dd3fc",
    selectionBackground: "#334155",
  },
  scrollback: 10000,
});
const fitAddon = new FitAddon();
terminal.loadAddon(fitAddon);
terminal.open($("#terminal"));
fitAddon.fit();

function setTerminalStatus(label, stateName = "") {
  const status = $("#terminal-status");
  status.textContent = label;
  status.className = `terminal-status ${stateName}`.trim();
}

terminal.onData((data) => {
  if (state.terminalSocket?.readyState === WebSocket.OPEN) {
    sendTerminalInput(state.terminalSocket, data);
  }
});

function rejectSocketAcks(socket, error) {
  for (const [id, pending] of state.terminalPendingAcks) {
    if (pending.socket !== socket) continue;
    window.clearTimeout(pending.timeout);
    pending.reject(error);
    state.terminalPendingAcks.delete(id);
  }
}

function sendTerminalInput(socket, data, { acknowledge = false } = {}) {
  if (socket.readyState !== WebSocket.OPEN) {
    throw new Error("terminal is not connected");
  }
  if (!acknowledge) {
    socket.send(JSON.stringify({ type: "input", data }));
    return Promise.resolve();
  }
  const id = `input-${Date.now()}-${++state.terminalInputSequence}`;
  return new Promise((resolve, reject) => {
    const timeout = window.setTimeout(() => {
      state.terminalPendingAcks.delete(id);
      reject(new Error("terminal did not acknowledge input"));
    }, 5000);
    state.terminalPendingAcks.set(id, {
      socket,
      timeout,
      resolve,
      reject,
    });
    try {
      socket.send(JSON.stringify({ type: "input", id, data }));
    } catch (error) {
      window.clearTimeout(timeout);
      state.terminalPendingAcks.delete(id);
      reject(error);
    }
  });
}

function sendSize() {
  fitAddon.fit();
  if (state.terminalSocket?.readyState === WebSocket.OPEN) {
    state.terminalSocket.send(
      JSON.stringify({ type: "resize", cols: terminal.cols, rows: terminal.rows }),
    );
  }
}
new ResizeObserver(sendSize).observe($("#terminal"));

function selectedLabel(thread) {
  return (thread.name || thread.preview || thread.id.slice(0, 12)).slice(0, 96);
}

function renderMessageModes(thread) {
  const select = $("#message-mode");
  const previous = select.value;
  select.replaceChildren();
  select.disabled = false;
  $("#submit-message").disabled = false;

  const addOption = (value, label) => {
    const option = document.createElement("option");
    option.value = value;
    option.textContent = label;
    select.append(option);
  };

  if (thread.archived) {
    addOption("unavailable", "Restore this thread before sending messages");
    select.disabled = true;
    $("#submit-message").disabled = true;
  } else if (
    thread.ownership === "sdk_turn" ||
    thread.status === "active"
  ) {
    addOption("steer", "Steer active shared turn");
    addOption("queue", "Queue after active turn");
  } else {
    addOption("send", "Start turn through shared app-server");
    addOption("queue", "Queue through atomic backend");
  }

  if ([...select.options].some((option) => option.value === previous)) {
    select.value = previous;
  }
}

function renderThreads() {
  const list = $("#thread-list");
  list.replaceChildren();
  for (const thread of state.threads) {
    const button = document.createElement("button");
    button.className = `thread-card ${state.selected?.id === thread.id ? "selected" : ""}`;
    const title = document.createElement("strong");
    title.textContent = selectedLabel(thread);
    const meta = document.createElement("span");
    meta.textContent = `${thread.status}${thread.draft ? " · draft" : ""} · ${thread.ownership} · ${thread.queue_open} queued`;
    const cwd = document.createElement("small");
    cwd.textContent = thread.cwd;
    button.append(title, meta, cwd);
    button.addEventListener("click", () => {
      void chooseThread(thread);
    });
    list.append(button);
  }
}

function selectThread(thread) {
  state.selected = thread;
  $("#selected-title").textContent = selectedLabel(thread);
  $("#ownership").textContent = thread.ownership;
  $("#attach").disabled = thread.archived;
  $("#interrupt-turn").disabled =
    thread.archived ||
    (thread.ownership !== "sdk_turn" && thread.status !== "active");
  $("#attach").textContent =
    state.terminalThreadId === thread.id && state.terminalSocket
      ? "Detach"
      : "Attach CLI";
  $("#archive-thread").disabled =
    !thread.archived &&
    (thread.ownership !== "idle" || thread.status !== "idle");
  $("#archive-thread").textContent = thread.archived ? "Restore" : "Archive";
  renderMessageModes(thread);
  renderThreads();
}

async function chooseThread(thread) {
  if (
    state.terminalSocket &&
    state.terminalThreadId !== thread.id
  ) {
    state.terminalGeneration += 1;
    await disconnectTerminal();
  }
  selectThread(thread);
}

async function refresh() {
  if (state.refreshing) {
    state.refreshPending = true;
    return;
  }
  state.refreshing = true;
  try {
    const archived = $("#archived").checked;
    const createdHere = $("#created-here").checked;
    const payload = await api(
      `/api/snapshot?archived=${archived}&created_here=${createdHere}`,
    );
    state.lastEventId = Math.max(state.lastEventId, payload.event_id || 0);
    state.threads = payload.threads;
    if (state.selected) {
      const updated = state.threads.find((item) => item.id === state.selected.id);
      if (updated) selectThread(updated);
      else {
        state.terminalGeneration += 1;
        await disconnectTerminal();
        state.selected = null;
        $("#selected-title").textContent = "No thread selected";
        setTerminalStatus("Disconnected");
        $("#attach").disabled = true;
        $("#interrupt-turn").disabled = true;
        $("#archive-thread").disabled = true;
      }
    } else if (state.threads.length) {
      selectThread(state.threads[0]);
    }
    renderThreads();
  } catch (error) {
    log("Refresh failed", error.message, "error");
  } finally {
    state.refreshing = false;
    if (state.refreshPending) {
      state.refreshPending = false;
      void refresh();
    }
  }
}

async function waitForIdle(threadId, timeoutMs = 10000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const status = await api(`/api/threads/${threadId}/status`);
    if (
      status.ownership === "idle" &&
      status.thread?.status === "idle"
    ) {
      return;
    }
    await new Promise((resolve) => window.setTimeout(resolve, 150));
  }
  throw new Error("Codex CLI did not finish detaching in time");
}

$("#new-thread").addEventListener("click", async () => {
  const name = window.prompt("Thread name (optional):", "");
  if (name === null) return;
  try {
    const payload = await api("/api/threads", {
      method: "POST",
      body: JSON.stringify({ name: name || null }),
    });
    log("Thread created", payload.thread);
    state.selected = payload.thread;
    await refresh();
  } catch (error) {
    log("Create failed", error.message);
  }
});

$("#archive-thread").addEventListener("click", async () => {
  if (!state.selected) return;
  try {
    state.terminalGeneration += 1;
    await disconnectTerminal();
    await waitForIdle(state.selected.id);
    if (state.selected.archived) {
      await api(`/api/threads/${state.selected.id}/restore`, { method: "POST" });
    } else {
      await api(`/api/threads/${state.selected.id}/archive`, { method: "POST" });
    }
    state.selected = null;
    await refresh();
  } catch (error) {
    log("Archive/restore failed", error.message);
  }
});

$("#interrupt-turn").addEventListener("click", async () => {
  if (
    !state.selected ||
    (
      state.selected.ownership !== "sdk_turn" &&
      state.selected.status !== "active"
    )
  ) {
    return;
  }
  try {
    const result = await api(
      `/api/threads/${state.selected.id}/interrupt`,
      { method: "POST" },
    );
    log("Interrupt committed", result);
    await refresh();
  } catch (error) {
    log("Interrupt failed", error.message);
  }
});

async function disconnectTerminal() {
  const socket = state.terminalSocket;
  if (!socket) return;
  if (socket.readyState !== WebSocket.CLOSED) {
    await new Promise((resolve) => {
      const timeout = window.setTimeout(resolve, 1500);
      socket.addEventListener(
        "close",
        () => {
          window.clearTimeout(timeout);
          resolve();
        },
        { once: true },
      );
      socket.close();
    });
  }
  if (state.terminalSocket === socket) {
    rejectSocketAcks(socket, new Error("terminal disconnected"));
    state.terminalSocket = null;
    state.terminalThreadId = null;
    state.terminalConnectPromise = null;
    setTerminalStatus("Disconnected");
  }
}

async function connectTerminal(thread) {
  if (
    state.terminalThreadId === thread.id &&
    state.terminalSocket?.readyState === WebSocket.OPEN
  ) {
    return state.terminalSocket;
  }
  if (
    state.terminalThreadId === thread.id &&
    state.terminalSocket?.readyState === WebSocket.CONNECTING &&
    state.terminalConnectPromise
  ) {
    return state.terminalConnectPromise;
  }
  const generation = ++state.terminalGeneration;
  if (state.terminalSocket) {
    await disconnectTerminal();
  }
  if (generation !== state.terminalGeneration) {
    throw new Error("terminal connection was superseded");
  }

  terminal.reset();
  fitAddon.fit();
  setTerminalStatus("Connecting…", "connecting");
  const protocol = location.protocol === "https:" ? "wss:" : "ws:";
  const dimensions = new URLSearchParams({
    cols: String(terminal.cols),
    rows: String(terminal.rows),
  });
  const socket = new WebSocket(
    `${protocol}//${location.host}/ws/terminal/${thread.id}?${dimensions}`,
  );
  socket.binaryType = "arraybuffer";
  state.terminalSocket = socket;
  state.terminalThreadId = thread.id;
  $("#attach").textContent = "Detach";

  let resolveReady;
  let rejectReady;
  let opened = false;
  let ready = false;
  state.terminalConnectPromise = new Promise((resolve, reject) => {
    resolveReady = resolve;
    rejectReady = reject;
    socket.addEventListener("open", () => {
      opened = true;
      if (state.terminalSocket === socket) {
        setTerminalStatus("Starting CLI…", "connecting");
      }
    });
    socket.addEventListener("error", () => {
      if (!ready) {
        if (state.terminalSocket === socket) {
          setTerminalStatus("Connection failed", "error");
        }
        reject(new Error("terminal WebSocket failed to connect"));
      }
    });
    socket.addEventListener("close", () => {
      if (!ready) {
        if (state.terminalSocket === socket) {
          setTerminalStatus("Connection failed", "error");
        }
        reject(
          new Error(
            opened
              ? "terminal closed before Codex CLI became ready"
              : "terminal closed before WebSocket connected",
          ),
        );
      }
    });
  });

  socket.addEventListener("message", (event) => {
    if (state.terminalSocket !== socket) return;
    if (typeof event.data === "string") {
      let payload;
      try {
        payload = JSON.parse(event.data);
      } catch (_error) {
        payload = null;
      }
      if (payload?.type === "terminal_ready") {
        ready = true;
        setTerminalStatus("Connected", "connected");
        sendSize();
        resolveReady(socket);
        return;
      }
      if (payload?.type === "input_ack" && typeof payload.id === "string") {
        const pending = state.terminalPendingAcks.get(payload.id);
        if (pending?.socket === socket) {
          window.clearTimeout(pending.timeout);
          state.terminalPendingAcks.delete(payload.id);
          pending.resolve();
        }
        return;
      }
      const detail = payload?.error?.message || event.data;
      setTerminalStatus("Terminal error", "error");
      log("Terminal", detail);
      if (!ready) rejectReady(new Error(detail));
    } else {
      terminal.write(new Uint8Array(event.data));
    }
  });
  socket.addEventListener("close", async () => {
    rejectSocketAcks(socket, new Error("terminal disconnected"));
    if (state.terminalSocket === socket) {
      state.terminalSocket = null;
      state.terminalThreadId = null;
      state.terminalConnectPromise = null;
      setTerminalStatus("Disconnected");
      if (state.selected?.id === thread.id) {
        $("#attach").textContent = "Attach CLI";
      }
      log("Terminal detached");
    }
  });
  return state.terminalConnectPromise;
}

$("#attach").addEventListener("click", async () => {
  if (
    state.terminalSocket &&
    state.terminalThreadId === state.selected?.id
  ) {
    state.terminalGeneration += 1;
    await disconnectTerminal();
    return;
  }
  if (!state.selected) return;
  try {
    await connectTerminal(state.selected);
  } catch (error) {
    log("Terminal attach failed", error.message);
  }
});

function composerTurnOptions() {
  const options = {};
  const textFields = {
    cwd: "#turn-cwd",
    model: "#turn-model",
    service_tier: "#turn-service-tier",
  };
  for (const [key, selector] of Object.entries(textFields)) {
    const value = $(selector).value.trim();
    if (value) options[key] = value;
  }
  const selects = {
    effort: "#turn-effort",
    personality: "#turn-personality",
    summary: "#turn-summary",
  };
  for (const [key, selector] of Object.entries(selects)) {
    const value = $(selector).value;
    if (value) options[key] = value;
  }
  const schema = $("#turn-output-schema").value.trim();
  if (schema) {
    const parsed = JSON.parse(schema);
    if (!parsed || Array.isArray(parsed) || typeof parsed !== "object") {
      throw new Error("output schema must be a JSON object");
    }
    options.output_schema = parsed;
  }
  return options;
}

$("#submit-message").addEventListener("click", async () => {
  if (!state.selected) return log("Select a thread first");
  const mode = $("#message-mode").value;
  const text = $("#message").value.trim();
  if (!text) return;
  try {
    const turnOptions = mode === "steer" ? {} : composerTurnOptions();
    const path =
      mode === "steer"
        ? "messages/steer"
        : mode === "queue"
          ? "messages/queue"
          : "messages/send";
    const result = await api(`/api/threads/${state.selected.id}/${path}`, {
      method: "POST",
      body: JSON.stringify({ text, ...turnOptions }),
    });
    $("#message").value = "";
    log(`${mode} committed`, result);
    await refresh();
  } catch (error) {
    log(`${mode} failed`, error.message);
  }
});

$("#refresh").addEventListener("click", refresh);
$("#archived").addEventListener("change", refresh);
$("#created-here").addEventListener("change", refresh);
$("#clear-log").addEventListener("click", () => {
  activityElement.replaceChildren();
});

function connectEvents() {
  if (
    state.eventsSocket?.readyState === WebSocket.OPEN ||
    state.eventsSocket?.readyState === WebSocket.CONNECTING
  ) {
    return;
  }
  if (state.eventsReconnectTimer !== null) {
    window.clearTimeout(state.eventsReconnectTimer);
    state.eventsReconnectTimer = null;
  }

  const protocol = location.protocol === "https:" ? "wss:" : "ws:";
  const cursor = new URLSearchParams({ since: String(state.lastEventId) });
  const socket = new WebSocket(
    `${protocol}//${location.host}/ws/events?${cursor}`,
  );
  state.eventsSocket = socket;
  setEventStatus("Connecting");
  socket.addEventListener("open", () => {
    state.eventsReconnectAttempt = 0;
    setEventStatus("Syncing");
  });
  socket.addEventListener("message", (event) => {
    state.eventProcessing = state.eventProcessing
      .then(async () => {
        const payload = JSON.parse(event.data);
        if (payload.type === "heartbeat") return;
        if (payload.type === "event_stream_ready") {
          state.lastEventId = Math.max(
            state.lastEventId,
            payload.latest_event_id || 0,
          );
          setEventStatus("Live", "live");
          return;
        }
        if (payload.type === "resync_required") {
          setEventStatus("Re-syncing");
          log("Event stream re-sync", payload, "error");
          await refresh();
          setEventStatus("Live", "live");
          return;
        }
        if (
          Number.isInteger(payload.event_id) &&
          payload.event_id <= state.lastEventId
        ) {
          return;
        }
        if (Number.isInteger(payload.event_id)) {
          state.lastEventId = payload.event_id;
        }
        log(eventLabel(payload), payload, "event");
        scheduleRefresh();
      })
      .catch((error) => {
        log("Event processing failed", error.message, "error");
      });
  });
  socket.addEventListener("close", () => {
    if (state.eventsSocket !== socket) return;
    state.eventsSocket = null;
    setEventStatus("Offline", "offline");
    const delay = Math.min(
      30000,
      1000 * 2 ** state.eventsReconnectAttempt,
    );
    state.eventsReconnectAttempt += 1;
    state.eventsReconnectTimer = window.setTimeout(() => {
      state.eventsReconnectTimer = null;
      connectEvents();
    }, delay);
  });
}

window.addEventListener("online", connectEvents);
window.setInterval(() => {
  if (
    document.visibilityState === "visible" &&
    state.selected &&
    state.terminalSocket?.readyState === WebSocket.OPEN
  ) {
    void refresh();
  }
}, 1500);

refresh();
connectEvents();
