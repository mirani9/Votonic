import http from "k6/http";
import { check, sleep } from "k6";

export const options = {
  stages: [
    { duration: "5s", target: 50 },
    { duration: "10s", target: 200 },
    { duration: "5s", target: 0 },
  ],
};

export default function () {
  const url = "http://localhost:3000/api/v1/poll/vote";
  const payload = JSON.stringify({
    candidateId: "Alex Morgan",
    userId: "vu_" + Math.floor(Math.random() * 1000000),
  });
  const params = { headers: { "Content-Type": "application.json" } };
  const res = http.post(url, payload, params);
  check(res, {
    "status is 200 or 429": (r) => r.status === 200 || r.status === 429,
  });
  sleep(0.05);
}
