const SW_VERSION = "timeclock-sw-v20260316-syncfix1";
const APP_SHELL_CACHE = `${SW_VERSION}-shell`;
const RUNTIME_CACHE = `${SW_VERSION}-runtime`;

const APP_SHELL_URLS = [
  "/",
  "/static/site.webmanifest",
  "/static/icons/apple-touch-icon-180.png",
  "/static/icons/apple-touch-icon-167.png",
  "/static/icons/apple-touch-icon-152.png",
  "/static/icons/favicon-32.png",
  "/static/vendor/face-api/face-api.js",
  "/static/vendor/face-api/model/tiny_face_detector_model-weights_manifest.json",
  "/static/vendor/face-api/model/tiny_face_detector_model-shard1",
  "/static/vendor/face-api/model/face_landmark_68_model-weights_manifest.json",
  "/static/vendor/face-api/model/face_landmark_68_model-shard1",
  "/static/vendor/face-api/model/face_recognition_model-weights_manifest.json",
  "/static/vendor/face-api/model/face_recognition_model-shard1",
  "/static/vendor/face-api/model/face_recognition_model-shard2",
];

const REQUIRED_PRECACHE_URLS = [
  "/",
  "/static/vendor/face-api/face-api.js",
  "/static/vendor/face-api/model/tiny_face_detector_model-weights_manifest.json",
  "/static/vendor/face-api/model/tiny_face_detector_model-shard1",
  "/static/vendor/face-api/model/face_landmark_68_model-weights_manifest.json",
  "/static/vendor/face-api/model/face_landmark_68_model-shard1",
  "/static/vendor/face-api/model/face_recognition_model-weights_manifest.json",
  "/static/vendor/face-api/model/face_recognition_model-shard1",
  "/static/vendor/face-api/model/face_recognition_model-shard2",
];

function isCacheableResponse(resp) {
  return !!resp && (resp.status === 200 || resp.type === "opaque");
}

async function putInCache(cacheName, request, response) {
  if (!isCacheableResponse(response)) return;
  const cache = await caches.open(cacheName);
  await cache.put(request, response.clone());
}

async function precacheShell() {
  const cache = await caches.open(APP_SHELL_CACHE);
  for (const url of APP_SHELL_URLS) {
    try {
      const resp = await fetch(url, { cache: "no-store" });
      if (isCacheableResponse(resp)) {
        await cache.put(url, resp.clone());
      }
    } catch (_) {
      // Ignore individual failures; runtime cache can populate later.
    }
  }
  const missing = [];
  for (const url of REQUIRED_PRECACHE_URLS) {
    // ignoreSearch=true allows HTML `?v=` query suffixes to reuse cached entries.
    const hit = await cache.match(url, { ignoreSearch: true });
    if (!hit) missing.push(url);
  }
  if (missing.length > 0) {
    throw new Error(`precache-missing-required-assets:${missing.length}`);
  }
}

async function cleanOldCaches() {
  const names = await caches.keys();
  await Promise.all(
    names
      // Keep previous timeclock caches as fallback for offline reliability.
      .filter((name) => !name.startsWith("timeclock-sw-v"))
      .map((name) => caches.delete(name))
  );
}

async function respondNavigation(request) {
  try {
    const networkResp = await fetch(request);
    await putInCache(RUNTIME_CACHE, request, networkResp);
    return networkResp;
  } catch (_) {
    const runtimeHit = await caches.match(request, { ignoreSearch: true });
    if (runtimeHit) return runtimeHit;
    const shellHit = await caches.match("/", { ignoreSearch: true });
    if (shellHit) return shellHit;
    return new Response("Offline", { status: 503, statusText: "Offline" });
  }
}

async function respondCacheFirst(request) {
  const cached = (await caches.match(request, { ignoreSearch: false })) || (await caches.match(request, { ignoreSearch: true }));
  if (cached) {
    fetch(request)
      .then((resp) => putInCache(RUNTIME_CACHE, request, resp))
      .catch(() => {});
    return cached;
  }
  const networkResp = await fetch(request);
  await putInCache(RUNTIME_CACHE, request, networkResp);
  return networkResp;
}

self.addEventListener("install", (event) => {
  event.waitUntil(precacheShell().then(() => self.skipWaiting()));
});

self.addEventListener("activate", (event) => {
  event.waitUntil(cleanOldCaches().then(() => self.clients.claim()));
});

self.addEventListener("message", (event) => {
  if (event.data && event.data.type === "SKIP_WAITING") {
    self.skipWaiting();
  }
});

self.addEventListener("fetch", (event) => {
  const request = event.request;
  if (request.method !== "GET") return;

  const url = new URL(request.url);
  const isSameOrigin = url.origin === self.location.origin;
  const isNavigation = request.mode === "navigate";
  const isStatic = isSameOrigin && url.pathname.startsWith("/static/");
  const isRoot = isSameOrigin && (url.pathname === "/" || url.pathname === "/sw.js");

  if (isNavigation) {
    event.respondWith(respondNavigation(request));
    return;
  }

  if (isStatic || isRoot) {
    event.respondWith(
      respondCacheFirst(request).catch(async () => {
        const fallback = await caches.match(request, { ignoreSearch: true });
        if (fallback) return fallback;
        throw new Error("offline-cache-miss");
      })
    );
  }
});
