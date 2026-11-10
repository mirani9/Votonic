class LoadBalancerEngine {
  constructor(targets = []) {
    this.targets = targets; // [{ url, instanceId, activeConns: 0, totalRequests: 0 }]
    this.rrIndex = 0;
  }

  getNextRoundRobin() {
    if (this.targets.length === 0) return null;
    const target = this.targets[this.rrIndex];
    this.rrIndex = (this.rrIndex + 1) % this.targets.length;
    target.totalRequests++;
    return target;
  }

  getNextLeastConnections() {
    if (this.targets.length === 0) return null;
    let minTarget = this.targets[0];
    for (const t of this.targets) {
      if (t.activeConns < minTarget.activeConns) {
        minTarget = t;
      }
    }
    minTarget.totalRequests++;
    return minTarget;
  }

  trackConnectionStart(target) {
    target.activeConns++;
  }

  trackConnectionEnd(target) {
    if (target.activeConns > 0) target.activeConns--;
  }

  getMetrics() {
    return this.targets.map(t => ({
      url: t.url,
      instanceId: t.instanceId,
      activeConnections: t.activeConns,
      totalRequests: t.totalRequests
    }));
  }
}

module.exports = LoadBalancerEngine;
