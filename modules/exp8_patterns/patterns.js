// 1. Adapter Pattern (Notification Services)
class EmailNotificationProvider {
  send(to, message) {
    return `[Email Adapter] Sent email to ${to}: ${message}`;
  }
}
class SMSNotificationProvider {
  sendSMS(phone, text) {
    return `[SMS Adapter] Sent SMS to ${phone}: ${text}`;
  }
}
class NotificationAdapter {
  constructor(provider, type = 'email') {
    this.provider = provider;
    this.type = type;
  }
  notify(recipient, msg) {
    if (this.type === 'email') return this.provider.send(recipient, msg);
    if (this.type === 'sms') return this.provider.sendSMS(recipient, msg);
  }
}

// 2. Observer Pattern (Poll Event Publisher)
class PollSubject {
  constructor() {
    this.observers = [];
  }
  subscribe(observer) {
    this.observers.push(observer);
  }
  notifyAll(eventData) {
    return this.observers.map(obs => obs.update(eventData));
  }
}
class LeaderboardObserver {
  update(event) {
    return `[Leaderboard Observer] Cache refreshed for candidate ${event.candidate_id}`;
  }
}
class AuditLogObserver {
  update(event) {
    return `[Audit Log Observer] Recorded vote event for voter ${event.voter_id}`;
  }
}

// 3. Strategy Pattern (Rate Limiting Selection)
class StrictRateLimitStrategy {
  isAllowed(requestCount) { return requestCount <= 5; }
}
class RelaxedRateLimitStrategy {
  isAllowed(requestCount) { return requestCount <= 100; }
}
class RateLimiterContext {
  constructor(strategy) {
    this.strategy = strategy;
  }
  setStrategy(strategy) {
    this.strategy = strategy;
  }
  executeStrategy(count) {
    return this.strategy.isAllowed(count);
  }
}

module.exports = {
  EmailNotificationProvider,
  SMSNotificationProvider,
  NotificationAdapter,
  PollSubject,
  LeaderboardObserver,
  AuditLogObserver,
  StrictRateLimitStrategy,
  RelaxedRateLimitStrategy,
  RateLimiterContext
};
