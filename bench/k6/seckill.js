// k6 load test for the seckill endpoint.
//
// Run:
//   k6 run bench/k6/seckill.js
//
// Tunables (env vars):
//   API_BASE   default http://localhost:8080
//   ACTIVITY   default 1001
//   USERS      default 10000

import http from 'k6/http';
import { check } from 'k6';
import { uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

export const options = {
  scenarios: {
    baseline_m1: {
      executor: 'constant-arrival-rate',
      rate: Number(__ENV.RATE || 1000),
      timeUnit: '1s',
      duration: __ENV.DURATION || '30s',
      preAllocatedVUs: 200,
      maxVUs: 1000,
    },
  },
  thresholds: {
    // M1 baseline — record only, do not gate. Tighten in M4.
    http_req_failed: ['rate<1'],
    http_req_duration: ['p(99)<1000'],
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
    headers: { 'Content-Type': 'application/json' },
  });
  check(res, {
    'status is queued or expected fail': (r) =>
      r.status === 202 || r.status === 409 || r.status === 410 || r.status === 403 || r.status === 404,
  });
}
