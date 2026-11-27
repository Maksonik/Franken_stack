const byId = (id) => document.getElementById(id);
const createControls = byId("create-controls");
const key = byId("key");
const paymentId = byId("payment-id");
const paymentStatus = byId("payment-status");
const outboxStatus = byId("outbox-status");
const kafkaStatus = byId("kafka-status");
const journal = byId("journal");
const partnerUrl = new URL("/webhooks/payments", window.location.origin);
partnerUrl.port = "8001";

let busy = false;
let webhookBody = null;
let outboxRefreshTimer;
let outboxReadVersion = 0;
const refreshInterval = 1000;
const refreshWindow = 30000;

function updateControls() {
  createControls.disabled = busy;
  paymentId.disabled = busy;
  for (const id of ["succeed", "race", "read-outbox"]) {
    byId(id).disabled = busy || !paymentId.value.trim();
  }
  for (const id of ["send-webhook", "fail-webhook"]) {
    byId(id).disabled = busy || webhookBody === null;
  }
  byId("clear").disabled = busy || journal.childElementCount === 0;
  byId("empty-journal").hidden = journal.childElementCount > 0;
}

async function runAction(action) {
  busy = true;
  updateControls();
  try {
    await action();
  } finally {
    busy = false;
    updateControls();
  }
}

function describePayload(raw) {
  if (!raw) return "Пустое тело";
  try {
    let containsLargeInteger = false;
    const data = JSON.parse(raw, (_, value) => {
      if (Number.isInteger(value) && !Number.isSafeInteger(value)) containsLargeInteger = true;
      return value;
    });
    return containsLargeInteger ? raw : JSON.stringify(data, null, 2);
  } catch {
    return raw;
  }
}

function addEntry({ method, url, headers, body, status, elapsed, response }) {
  const entry = byId("request-row").content.firstElementChild.cloneNode(true);
  const address = new URL(url, window.location.origin);
  const route = address.origin === window.location.origin
    ? `${address.pathname}${address.search}`
    : `${address.host}${address.pathname}${address.search}`;
  const badge = entry.querySelector(".http-status");
  badge.textContent = status ?? "Ошибка сети";
  badge.dataset.tone = status >= 200 && status < 300 ? "success" : "error";
  entry.querySelector(".route").textContent = `${method} ${route}`;
  entry.querySelector(".duration").textContent = `${elapsed} мс`;

  const headerLines = Object.entries(headers).map(([name, value]) => `${name}: ${value}`);
  entry.querySelector(".sent pre").textContent = [...headerLines, describePayload(body)].join("\n");
  entry.querySelector(".received").textContent = describePayload(response);
  if (journal.firstElementChild) journal.firstElementChild.open = false;
  entry.open = true;
  journal.prepend(entry);
  updateControls();
}

async function request(method, url, { body, idempotencyKey, log = true } = {}) {
  const headers = {};
  if (body !== undefined) headers["Content-Type"] = "application/json";
  if (idempotencyKey !== undefined) headers["Idempotency-Key"] = idempotencyKey;
  const started = performance.now();
  let status = null;
  let responseHeaders = new Headers();
  let raw;
  try {
    const response = await fetch(url, { method, headers, body, signal: AbortSignal.timeout(15000) });
    raw = await response.text();
    status = response.status;
    responseHeaders = response.headers;
  } catch (error) {
    raw = error.message;
  }

  if (log) {
    addEntry({ method, url, headers, body, status, elapsed: Math.round(performance.now() - started), response: raw });
  }
  return { ok: status >= 200 && status < 300, raw, responseHeaders };
}

function selectedId() {
  return encodeURIComponent(paymentId.value.trim());
}

function showPaymentStatus(status) {
  paymentStatus.textContent = status ?? "Статус неизвестен";
  paymentStatus.dataset.tone = status === "pending" ? "pending" : status === "succeeded" ? "success" : "";
}

function resetOutbox() {
  clearTimeout(outboxRefreshTimer);
  outboxReadVersion++;
  webhookBody = null;
  kafkaStatus.textContent = "Kafka · событие появится после подтверждения платежа";
  outboxStatus.textContent = selectedId()
    ? "Обнови, чтобы прочитать события выбранного платежа."
    : "Выбери платёж, чтобы прочитать его события.";
}

function scheduleOutboxRefresh(deadline) {
  if (performance.now() + refreshInterval < deadline) {
    outboxRefreshTimer = setTimeout(() => readOutbox({ background: true, deadline }), refreshInterval);
  }
}

async function readOutbox({ background = false, deadline = performance.now() + refreshWindow } = {}) {
  clearTimeout(outboxRefreshTimer);
  const version = ++outboxReadVersion;
  const id = selectedId();
  if (!id) return;

  const { ok, raw, responseHeaders } = await request("GET", `/lab/payments/${id}/outbox`, { log: !background });
  if (version !== outboxReadVersion || id !== selectedId()) return;

  if (!ok) {
    if (background) {
      scheduleOutboxRefresh(deadline);
    } else {
      webhookBody = null;
      outboxStatus.textContent = "Ошибка чтения outbox. Посмотри ответ в журнале.";
      kafkaStatus.textContent = "Kafka · статус события недоступен";
    }
    return;
  }

  const event = JSON.parse(raw);
  if (event === null) {
    webhookBody = null;
    outboxStatus.textContent = "0 событий. Подтверди платёж, чтобы записать payment.succeeded.";
    kafkaStatus.textContent = "Kafka · событие появится после подтверждения платежа";
    return;
  }

  // Forward the original JSON to preserve bigint amounts.
  webhookBody = raw;
  const publishedAt = responseHeaders.get("X-Outbox-Published-At");
  const receivedAt = responseHeaders.get("X-Webhook-Received-At");
  const publication = publishedAt ? "RabbitMQ подтвердил публикацию" : "Ожидает публикации в RabbitMQ";
  const receipt = receivedAt ? "Партнёр принял событие" : "Ожидается приём у партнёра";
  outboxStatus.textContent = `1 событие · ${event.type} · ${event.id} · ${publication} · ${receipt}`;
  const groups = (responseHeaders.get("X-Kafka-Consumer-Groups") ?? "").split(", ");
  const waitingGroups = ["analytics", "audit"].filter((group) => !groups.includes(group));
  if (waitingGroups.length === 0) {
    kafkaStatus.textContent = "Kafka · прочитали analytics, audit";
  } else if (responseHeaders.has("X-Kafka-Published-At")) {
    kafkaStatus.textContent = `Kafka · опубликовано · ожидаем ${waitingGroups.join(", ")}`;
  } else {
    kafkaStatus.textContent = "Kafka · ожидает публикации";
  }
  updateControls();
  if (!publishedAt || !receivedAt || waitingGroups.length > 0) scheduleOutboxRefresh(deadline);
}

async function succeed(times) {
  const url = `/payments/${selectedId()}/succeed`;
  const results = await Promise.all(Array.from({ length: times }, () => request("POST", url)));
  const success = results.find((result) => result.ok);
  if (success) showPaymentStatus(JSON.parse(success.raw).status);
  await readOutbox();
}

function newKey() {
  key.value = `lab-${crypto.randomUUID()}`;
}

byId("create-form").addEventListener("submit", (event) => {
  event.preventDefault();
  const amount = byId("amount").value;
  const currency = JSON.stringify(byId("currency").value);
  const body = `{"amount_minor":${amount},"currency":${currency}}`;
  runAction(async () => {
    const { ok, raw } = await request("POST", "/payments", { body, idempotencyKey: key.value });
    if (ok) {
      const payment = JSON.parse(raw);
      if (payment.id !== paymentId.value) {
        paymentId.value = payment.id;
        resetOutbox();
      }
      showPaymentStatus(payment.status);
    }
  });
});

byId("new-key").addEventListener("click", newKey);
paymentId.addEventListener("input", () => {
  showPaymentStatus(null);
  resetOutbox();
  updateControls();
});
byId("succeed").addEventListener("click", () => runAction(() => succeed(1)));
byId("race").addEventListener("click", () => runAction(() => succeed(2)));
byId("read-outbox").addEventListener("click", () => runAction(readOutbox));
byId("send-webhook").addEventListener("click", () => runAction(() => request("POST", partnerUrl, { body: webhookBody })));
byId("fail-webhook").addEventListener("click", () => {
  const url = new URL(partnerUrl);
  url.searchParams.set("mode", "unavailable");
  runAction(() => request("POST", url, { body: webhookBody }));
});
byId("clear").addEventListener("click", () => {
  journal.replaceChildren();
  updateControls();
});

newKey();
updateControls();
