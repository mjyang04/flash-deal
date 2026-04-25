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
    burst: {
      executor: 'ramping-arrival-rate',
      startRate: 100,
      timeUnit: '1s',
      preAllocatedVUs: 500,
      maxVUs: 4000,
      stages: [
        { target: 5000,  duration: '30s' },
        { target: 30000, duration: '60s' },
        { target: 30000, duration: '60s' },
        { target: 0,     duration: '15s' },
      ],
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(99)<50'],
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
    'status is 200 or 4xx (expected)': (r) => r.status === 200 || r.status === 409 || r.status === 410,
  });
}
