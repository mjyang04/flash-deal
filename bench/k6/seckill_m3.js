// M3 baseline (sharded MySQL + ratelimit + idempotency + metrics).
// Run:
//   RATE=1000 DURATION=30s k6 run bench/k6/seckill_m3.js

import http from 'k6/http';
import { check } from 'k6';
import { uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

export const options = {
  scenarios: {
    m3_baseline: {
      executor: 'constant-arrival-rate',
      rate: Number(__ENV.RATE || 1000),
      timeUnit: '1s',
      duration: __ENV.DURATION || '30s',
      preAllocatedVUs: 200,
      maxVUs: 1000,
    },
  },
  thresholds: {
    http_req_failed: ['rate<1'],
    http_req_duration: ['p(99)<500'],
  },
};

const BASE = __ENV.API_BASE || 'http://localhost:8080';
const ACTIVITY = Number(__ENV.ACTIVITY || 1001);
const USERS = Number(__ENV.USERS || 10000);

export default function () {
  const userID = Math.floor(Math.random() * USERS) + 1;
  const body = JSON.stringify({
    activity_id: ACTIVITY,
    user_id: userID,
    idempotency_token: uuidv4(),
  });
  const res = http.post(`${BASE}/v1/seckill`, body, {
    headers: { 'Content-Type': 'application/json', 'X-User-Id': String(userID) },
  });
  check(res, {
    'status is queued or expected fail': (r) =>
      r.status === 202 || r.status === 409 || r.status === 410 ||
      r.status === 403 || r.status === 404 || r.status === 429,
  });
}
